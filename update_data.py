import csv
import json
import os
import re
import collections
from datetime import datetime, timedelta, timezone
import traceback
from urllib.parse import urlparse, parse_qs

try:
    import requests
except ImportError:
    requests = None

try:
    from snownlp import SnowNLP
except ImportError:
    SnowNLP = None

try:
    from ckip_transformers.nlp import CkipWordSegmenter
except ImportError:
    CkipWordSegmenter = None

# ==========================================
# 1. 系統環境設定區
# ==========================================
OUTPUT_JSON_PATH = 'data.json'
POST_LOOKBACK_DAYS = 90
POSTS_PER_REQUEST = min(int(os.environ.get('POSTS_PER_REQUEST', '10')), 10)
MAX_POST_PAGES = int(os.environ.get('MAX_POST_PAGES', '200'))
SENTIMENT_POSITIVE_THRESHOLD = float(os.environ.get('SENTIMENT_POSITIVE_THRESHOLD', '0.65'))
SENTIMENT_NEGATIVE_THRESHOLD = float(os.environ.get('SENTIMENT_NEGATIVE_THRESHOLD', '0.35'))
CKIP_MODEL = os.environ.get('CKIP_MODEL', 'bert-base')
CKIP_DEVICE = int(os.environ.get('CKIP_DEVICE', '-1'))
ENABLE_LLM_ANALYSIS = os.environ.get('ENABLE_LLM_ANALYSIS', 'false').lower() in {'1', 'true', 'yes'}
OPENAI_MODEL = os.environ.get('OPENAI_MODEL', 'gpt-5-nano')
OPENAI_API_URL = os.environ.get('OPENAI_API_URL', 'https://api.openai.com/v1/responses')
LLM_POST_LIMIT = int(os.environ.get('LLM_POST_LIMIT', '1000'))
LLM_COMMENT_POST_LIMIT = int(os.environ.get('LLM_COMMENT_POST_LIMIT', '5'))
LLM_COMMENTS_PER_POST = int(os.environ.get('LLM_COMMENTS_PER_POST', '20'))

TW_TZ = timezone(timedelta(hours=8))
WEEKDAY_MAP = {0: "週一", 1: "週二", 2: "週三", 3: "週四", 4: "週五", 5: "週六", 6: "週日"}
STOP_WORDS = {
    '的', '是', '在', '了', '與', '和', '也', '有', '就', '我', '這', '都', '及', '為', '讓', '於', '以',
    '對', '我們', '大家', '一個', '可以', '不', '很', '會', '到', '上', '但', '那', '你', '他', '她'
}
CKIP_WS_DRIVER = None
CKIP_WS_INIT_ATTEMPTED = False

def safe_int(val):
    try: return int(val) if val is not None else 0
    except (ValueError, TypeError): return 0

def parse_post_datetime(post):
    created_at = post.get('createdAt') or post.get('created_at') or post.get('creationTime')
    if not created_at or not isinstance(created_at, str):
        return None

    try:
        normalized = created_at.replace('Z', '+00:00')
        post_dt = datetime.fromisoformat(normalized)
        if post_dt.tzinfo is None:
            post_dt = post_dt.replace(tzinfo=timezone.utc)
        return post_dt.astimezone(timezone.utc)
    except ValueError:
        pass

    if len(created_at) >= 19:
        try:
            return datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None

def get_post_text(post):
    return post.get('text') or post.get('message') or post.get('description') or ''

def get_post_url(post):
    return post.get('permalinkUrl') or post.get('url')

def calculate_interactions(post):
    likes = safe_int(post.get('reactionCount') or post.get('likeCount') or post.get('likes'))
    comments = safe_int(post.get('commentCount') or post.get('comments'))
    shares = safe_int(post.get('shareCount') or post.get('shares'))
    return likes + comments + shares

def calculate_comment_count(post):
    return safe_int(post.get('commentCount') or post.get('comments'))

def post_date_string(post):
    post_dt = parse_post_datetime(post)
    if post_dt:
        return post_dt.astimezone(TW_TZ).strftime("%Y-%m-%d")
    created_at = post.get('createdAt') or post.get('created_at')
    return created_at[:10] if created_at else "未知"

def get_bycrawl_headers():
    api_key = os.environ.get('BYCRAWL_API_KEY')
    if not api_key:
        raise RuntimeError("缺少 BYCRAWL_API_KEY，請在環境變數或 GitHub Secrets 設定，不要寫進原始碼。")
    return {"x-api-key": api_key}

def get_openai_headers():
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        raise RuntimeError("缺少 OPENAI_API_KEY，請在環境變數或 GitHub Secrets 設定，不要寫進原始碼。")
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

def get_word_segmenter():
    global CKIP_WS_DRIVER, CKIP_WS_INIT_ATTEMPTED
    if CKIP_WS_DRIVER is not None:
        return CKIP_WS_DRIVER
    if CKIP_WS_INIT_ATTEMPTED or CkipWordSegmenter is None:
        return None

    CKIP_WS_INIT_ATTEMPTED = True
    try:
        CKIP_WS_DRIVER = CkipWordSegmenter(model=CKIP_MODEL, device=CKIP_DEVICE)
    except Exception as e:
        print(f"  [警告] CKIP Transformers 初始化失敗，略過關鍵字斷詞: {e}")
        CKIP_WS_DRIVER = None
    return CKIP_WS_DRIVER

def tokenize_text(text):
    segmenter = get_word_segmenter()
    if segmenter is None or not text:
        return []
    try:
        segmented = segmenter([text])
        return segmented[0] if segmented else []
    except Exception as e:
        print(f"  [警告] CKIP Transformers 斷詞失敗，略過此段文字: {e}")
        return []

# ==========================================
# 2. 核心抓取模組
# ==========================================
def extract_fb_id(url):
    if not url or not isinstance(url, str): return None
    url = url.strip()
    parsed_url = urlparse(url)
    if 'profile.php' in parsed_url.path:
        params = parse_qs(parsed_url.query)
        if 'id' in params: return params['id'][0]
    clean_url = url.split('?')[0].strip().rstrip('/')
    if '/p/' in clean_url:
        match = re.search(r'-(\d+)$', clean_url)
        if match: return match.group(1)
    parts = clean_url.split('/')
    if len(parts) > 0: return parts[-1]
    return clean_url

def fetch_page_profile(username):
    if not username: return None
    if requests is None:
        raise RuntimeError("缺少 requests 套件，請先安裝 requirements.txt。")
    headers = get_bycrawl_headers()
    url = f"https://api.bycrawl.com/facebook/users/{username}"
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        res_json = response.json()
        if res_json.get("success") is False: return None
        return res_json.get("data", res_json)
    except Exception: return None

def fetch_page_posts_90_days(username):
    posts, _ = fetch_page_posts_90_days_with_meta(username)
    return posts

def fetch_page_posts_90_days_with_meta(username):
    if not username:
        return [], {
            "requestedDays": POST_LOOKBACK_DAYS,
            "postsPerRequest": POSTS_PER_REQUEST,
            "maxPages": MAX_POST_PAGES,
            "rawFetchedPosts": 0,
            "validPosts": 0,
            "stoppedReason": "missing_username",
            "sawOlderThanCutoff": False,
            "isComplete90Days": False,
            "oldestFetchedAt": None,
            "newestFetchedAt": None
        }
    if requests is None:
        raise RuntimeError("缺少 requests 套件，請先安裝 requirements.txt。")
    headers = get_bycrawl_headers()
    all_posts = []
    cursor = None
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=POST_LOOKBACK_DAYS)
    seen_older_than_cutoff = False
    stopped_reason = "max_pages_reached"
    
    for page_number in range(1, MAX_POST_PAGES + 1):
        url = f"https://api.bycrawl.com/facebook/users/{username}/posts"
        params = {"count": POSTS_PER_REQUEST}
        if cursor: params["cursor"] = cursor
            
        try:
            response = requests.get(url, params=params, headers=headers, timeout=45)
            response.raise_for_status()
            res_json = response.json()
            
            if res_json.get("success") is False:
                stopped_reason = "api_success_false"
                break
            data_block = res_json.get("data", res_json)
            posts = data_block.get('posts', [])
            if not posts:
                stopped_reason = "no_posts"
                break
            
            all_posts.extend(posts)
            cursor = data_block.get('nextCursor') or data_block.get('next_cursor') or data_block.get('cursor')

            dated_posts = [parse_post_datetime(p) for p in posts]
            dated_posts = [d for d in dated_posts if d is not None]
            if dated_posts and min(dated_posts) < cutoff_date:
                seen_older_than_cutoff = True
            if dated_posts and max(dated_posts) < cutoff_date:
                stopped_reason = "older_than_cutoff"
                break
            if not cursor:
                stopped_reason = "no_next_cursor"
                break
            
        except Exception as e:
            print(f"  [警告] 翻頁發生異常: {e}")
            stopped_reason = f"error: {e}"
            break

    valid_posts = []
    
    for p in all_posts:
        post_dt = parse_post_datetime(p)
        if post_dt is None or post_dt >= cutoff_date:
            valid_posts.append(p)

    dated_all_posts = [parse_post_datetime(p) for p in all_posts]
    dated_all_posts = [d for d in dated_all_posts if d is not None]
    meta = {
        "requestedDays": POST_LOOKBACK_DAYS,
        "postsPerRequest": POSTS_PER_REQUEST,
        "maxPages": MAX_POST_PAGES,
        "rawFetchedPosts": len(all_posts),
        "validPosts": len(valid_posts),
        "stoppedReason": stopped_reason,
        "sawOlderThanCutoff": seen_older_than_cutoff,
        "isComplete90Days": stopped_reason in {"older_than_cutoff", "no_next_cursor", "no_posts"},
        "oldestFetchedAt": min(dated_all_posts).astimezone(TW_TZ).isoformat() if dated_all_posts else None,
        "newestFetchedAt": max(dated_all_posts).astimezone(TW_TZ).isoformat() if dated_all_posts else None
    }

    return valid_posts, meta

def fetch_post_comments(post_url):
    if not post_url: return []
    if requests is None:
        raise RuntimeError("缺少 requests 套件，請先安裝 requirements.txt。")
    headers = get_bycrawl_headers()
    url = "https://api.bycrawl.com/facebook/posts/comments"
    try:
        response = requests.get(url, params={"url": post_url}, headers=headers, timeout=30)
        response.raise_for_status()
        res_json = response.json()
        if res_json.get("success") is False: return []
        data_block = res_json.get("data", res_json)
        return data_block.get('comments', [])
    except Exception: return []

POST_SEMANTIC_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "topic": {
            "type": "string",
            "enum": ["活動宣傳", "政策公告", "生態保育", "環境教育", "旅遊資訊", "災害封閉管制", "成果回顧", "民眾互動徵件", "其他"]
        },
        "tone": {
            "type": "string",
            "enum": ["正向鼓勵", "中性資訊", "提醒警示", "負向危機", "其他"]
        },
        "intent": {
            "type": "string",
            "enum": ["宣傳活動", "傳達政策", "教育知識", "提醒注意", "形象經營", "引導參與", "危機說明", "其他"]
        },
        "entities": {
            "type": "array",
            "items": {"type": "string"}
        },
        "summary": {"type": "string"}
    },
    "required": ["topic", "tone", "intent", "entities", "summary"]
}

COMMENT_SEMANTIC_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "mainConcern": {
            "type": "string",
            "enum": ["活動詢問", "交通與開放資訊", "保育議題", "正向回饋", "抱怨或疑慮", "標註分享", "無明確主題", "其他"]
        },
        "audienceSentiment": {
            "type": "string",
            "enum": ["正向", "中性", "負向", "混合", "樣本不足"]
        },
        "actionableInsight": {"type": "string"},
        "sampleSize": {"type": "integer"}
    },
    "required": ["mainConcern", "audienceSentiment", "actionableInsight", "sampleSize"]
}

def extract_response_text(response_json):
    if response_json.get("output_text"):
        return response_json["output_text"]
    for output in response_json.get("output", []):
        for content in output.get("content", []):
            if "text" in content:
                return content["text"]
    return ""

def call_openai_structured(prompt, schema, schema_name):
    if requests is None:
        raise RuntimeError("缺少 requests 套件，請先安裝 requirements.txt。")

    body = {
        "model": OPENAI_MODEL,
        "input": [
            {
                "role": "system",
                "content": "你是台灣正體中文社群內容分析助理。請只根據輸入文字分類，不要臆測，並輸出符合 JSON schema 的結果。"
            },
            {"role": "user", "content": prompt}
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "strict": True,
                "schema": schema
            }
        },
        "max_output_tokens": 500
    }

    response = requests.post(OPENAI_API_URL, headers=get_openai_headers(), json=body, timeout=60)
    response.raise_for_status()
    text = extract_response_text(response.json())
    return json.loads(text)

def analyze_post_semantics(post):
    text = get_post_text(post)
    if not text:
        return None
    prompt = (
        "請分析以下 Facebook 粉專貼文，分類其主題、語氣、溝通意圖、提及實體，並以一句話摘要。\n\n"
        f"粉專：{post.get('page_name', '未知粉專')}\n"
        f"類型：{post.get('page_type', '未知類型')}\n"
        f"貼文：{text[:2000]}"
    )
    return call_openai_structured(prompt, POST_SEMANTIC_SCHEMA, "post_semantic_analysis")

def analyze_comment_semantics(post, comments):
    sampled_comments = []
    for c in comments[:LLM_COMMENTS_PER_POST]:
        c_text = c.get('text') or c.get('message') or ''
        if c_text:
            sampled_comments.append(c_text[:300])
    if not sampled_comments:
        return None

    prompt = (
        "請分析以下高互動 Facebook 貼文的留言樣本，歸納主要關切、受眾情緒與可行洞察。\n\n"
        f"粉專：{post.get('page_name', '未知粉專')}\n"
        f"貼文摘要：{get_post_text(post)[:600]}\n"
        "留言樣本：\n"
        + "\n".join(f"- {comment}" for comment in sampled_comments)
    )
    result = call_openai_structured(prompt, COMMENT_SEMANTIC_SCHEMA, "comment_semantic_analysis")
    result["sampleSize"] = len(sampled_comments)
    return result

def summarize_semantic_results(post_results, comment_results):
    topic_counts = collections.Counter(r["topic"] for r in post_results if r)
    tone_counts = collections.Counter(r["tone"] for r in post_results if r)
    intent_counts = collections.Counter(r["intent"] for r in post_results if r)
    entity_counts = collections.Counter(
        entity
        for r in post_results if r
        for entity in r.get("entities", [])
        if entity
    )

    return {
        "enabled": True,
        "model": OPENAI_MODEL,
        "postSampleCount": len(post_results),
        "commentPostSampleCount": len(comment_results),
        "topicCounts": [{"label": k, "count": v} for k, v in topic_counts.most_common()],
        "toneCounts": [{"label": k, "count": v} for k, v in tone_counts.most_common()],
        "intentCounts": [{"label": k, "count": v} for k, v in intent_counts.most_common()],
        "topEntities": [{"label": k, "count": v} for k, v in entity_counts.most_common(20)],
        "postResults": post_results,
        "commentResults": comment_results
    }

def run_llm_semantic_analysis(posts, comments_by_post_url):
    if not ENABLE_LLM_ANALYSIS:
        return {"enabled": False, "reason": "ENABLE_LLM_ANALYSIS is false"}

    post_results = []
    for index, post in enumerate(posts[:LLM_POST_LIMIT], start=1):
        try:
            result = analyze_post_semantics(post)
            if result:
                result["page"] = post.get("page_name", "未知粉專")
                result["date"] = post_date_string(post)
                result["interactions"] = post.get("total_interactions", 0)
                post_results.append(result)
        except Exception as e:
            print(f"  [警告] LLM 貼文語意分析失敗 ({index}): {e}")

    comment_results = []
    comment_target_posts = sorted(
        [p for p in posts if get_post_url(p) in comments_by_post_url],
        key=lambda p: (safe_int(p.get('commentCount') or p.get('comments')), p.get('total_interactions', 0)),
        reverse=True
    )[:LLM_COMMENT_POST_LIMIT]

    for index, post in enumerate(comment_target_posts, start=1):
        try:
            result = analyze_comment_semantics(post, comments_by_post_url.get(get_post_url(post), []))
            if result:
                result["page"] = post.get("page_name", "未知粉專")
                result["date"] = post_date_string(post)
                result["interactions"] = post.get("total_interactions", 0)
                comment_results.append(result)
        except Exception as e:
            print(f"  [警告] LLM 留言語意分析失敗 ({index}): {e}")

    return summarize_semantic_results(post_results, comment_results)

# ==========================================
# 3. 數據分析與即時存檔模組
# ==========================================
def analyze_sentiment(text_list):
    if SnowNLP is None:
        return None
    if not text_list:
        return None
    positive_count = 0
    neutral_count = 0
    negative_count = 0
    valid_text_count = 0
    for text in text_list:
        if not isinstance(text, str) or not text.strip(): continue
        try:
            if len(text.strip()) > 2:
                score = SnowNLP(text).sentiments
                if score >= SENTIMENT_POSITIVE_THRESHOLD:
                    positive_count += 1
                elif score <= SENTIMENT_NEGATIVE_THRESHOLD:
                    negative_count += 1
                else:
                    neutral_count += 1
                valid_text_count += 1
        except Exception: pass
        
    if valid_text_count == 0: return None
    pos_ratio = round((positive_count / valid_text_count) * 100)
    neutral_ratio = round((neutral_count / valid_text_count) * 100)
    neg_ratio = max(0, 100 - pos_ratio - neutral_ratio)
    return {
        "positive": pos_ratio,
        "neutral": neutral_ratio,
        "negative": neg_ratio,
        "sampleCount": valid_text_count,
        "method": "SnowNLP 三分法",
        "thresholds": {
            "positive": SENTIMENT_POSITIVE_THRESHOLD,
            "negative": SENTIMENT_NEGATIVE_THRESHOLD
        }
    }

def save_progress(data):
    """防斷線存檔機制：每處理完一個粉專就存檔一次"""
    try:
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"  [警告] 儲存進度發生錯誤: {e}")

def get_csv_file():
    for file in os.listdir('.'):
        if file.endswith('.csv'): return file
    return None

def collect_text_signals(text, hashtags, keywords):
    if not text:
        return

    hashtags.extend(re.findall(r'#([^\s#]+)', text))

    text_clean = re.sub(r'#\S+', '', text)
    for w in tokenize_text(text_clean):
        w = w.strip()
        if len(w) > 1 and w not in STOP_WORDS and not w.encode().isalpha():
            keywords.append(w)

def summarize_content(posts, comment_texts=None):
    post_texts = []
    hour_interactions = {}
    all_hashtags = []
    all_keywords = []
    top_posts_data = []

    sorted_posts = sorted(posts, key=lambda x: x.get('total_interactions', 0), reverse=True)

    for sp in sorted_posts:
        text = get_post_text(sp)
        if text:
            post_texts.append(text)
            collect_text_signals(text, all_hashtags, all_keywords)

        post_dt = parse_post_datetime(sp)
        dt_tw = post_dt.astimezone(TW_TZ) if post_dt else None
        interactions = sp.get('total_interactions', 0)
        if dt_tw:
            key = f"{WEEKDAY_MAP[dt_tw.weekday()]} {dt_tw.hour:02d}:00"
            hour_interactions[key] = hour_interactions.get(key, 0) + interactions

        if len(top_posts_data) < 10:
            followers = sp.get('page_followers', 0)
            er_pct = round((interactions / followers) * 100, 2) if followers > 0 else 0
            top_posts_data.append({
                "page": sp.get('page_name', '未知粉專'),
                "date": post_date_string(sp),
                "content": str(text)[:60] + '...' if text else "無文字內容",
                "interactions": interactions,
                "interactionsText": f"{interactions:,}",
                "er": f"{er_pct}%"
            })

    best_time_str = max(hour_interactions, key=hour_interactions.get) if hour_interactions else "資料不足"
    avg_daily = round(len(posts) / POST_LOOKBACK_DAYS, 2)
    post_sentiment = analyze_sentiment(post_texts)
    comment_sentiment = analyze_sentiment(comment_texts or [])

    return {
        "avgDailyPosts": avg_daily,
        "bestPostingTime": best_time_str,
        "sentimentPostPositive": post_sentiment["positive"] if post_sentiment else None,
        "sentimentPostNeutral": post_sentiment["neutral"] if post_sentiment else None,
        "sentimentPostNegative": post_sentiment["negative"] if post_sentiment else None,
        "sentimentPostSampleCount": post_sentiment["sampleCount"] if post_sentiment else 0,
        "commentSentimentPositive": comment_sentiment["positive"] if comment_sentiment else None,
        "commentSentimentNeutral": comment_sentiment["neutral"] if comment_sentiment else None,
        "commentSentimentNegative": comment_sentiment["negative"] if comment_sentiment else None,
        "commentSentimentSampleCount": comment_sentiment["sampleCount"] if comment_sentiment else 0,
        "sentimentMethod": post_sentiment["method"] if post_sentiment else "SnowNLP 三分法",
        "topHashtags": [{"tag": f"#{k}", "count": v} for k, v in collections.Counter(all_hashtags).most_common(10)],
        "topKeywords": [{"word": k, "count": v} for k, v in collections.Counter(all_keywords).most_common(10)],
        "topPosts": top_posts_data
    }

def main():
    print(f"啟動社群數據更新排程 (邊抓邊存進階版)... (執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    csv_file = get_csv_file()
    if not csv_file:
        print("❌ 找不到 .csv 檔案！")
        return

    
    final_data = {
        "lastUpdated": datetime.now(TW_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "dataScope": f"所有成功抓取粉專最近 {POST_LOOKBACK_DAYS} 天活躍貼文",
        "basePage": {},
        "contentAnalysis": {},
        "allPages": [],
        "incompletePages": [],
        "skippedPages": []
    }

    pages = []
    try:
        with open(csv_file, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader: pages.append(row)
    except Exception as e:
        print(f"❌ 讀取 CSV 發生錯誤: {e}")
        return

    all_recent_posts = []
    base_recent_posts = []

    for page in pages:
        try:
            page_type = str(page.get('類型', '未知類型')).strip()
            page_name = str(page.get('粉專名稱', '未命名')).strip()
            page_url = str(page.get('網址', '')).strip()
            
            if not page_url: continue
                
            username = extract_fb_id(page_url)
            print(f"\n🔄 處理中: {page_name} (類型: {page_type} | ID: {username})")
            
            profile_data = fetch_page_profile(username)
            if not profile_data: 
                print("  [警告] 取無 profile_data，跳過此粉專。")
                final_data['skippedPages'].append({"name": page_name, "type": page_type, "url": page_url, "reason": "profile_data unavailable"})
                save_progress(final_data)
                continue
            
            followers = safe_int(profile_data.get('followerCount', profile_data.get('likesCount')))
            if followers == 0: 
                print("  [警告] 粉絲數為 0，跳過此粉專以防止計算錯誤。")
                final_data['skippedPages'].append({"name": page_name, "type": page_type, "url": page_url, "reason": "followers is 0"})
                save_progress(final_data)
                continue
                
            posts, fetch_meta = fetch_page_posts_90_days_with_meta(username)
            total_interactions = 0
            valid_posts_count = len(posts)
            
            for p in posts:
                interactions = calculate_interactions(p)
                total_interactions += interactions
                p['total_interactions'] = interactions
                p['page_name'] = page_name
                p['page_type'] = page_type
                p['page_followers'] = followers
                all_recent_posts.append(p)
                
            if followers > 0 and valid_posts_count > 0:
                post_engagement = round((total_interactions / (followers * valid_posts_count)) * 100, 4)
                page_engagement = round((total_interactions / followers) * 100, 2)
            else:
                post_engagement = 0
                page_engagement = 0
            
            print(f"  └ 粉絲: {followers:,} | 90天內貼文: {valid_posts_count} | 總互動: {total_interactions} | 粉專互動率: {page_engagement}%")
            if not fetch_meta["isComplete90Days"]:
                print(f"  [警告] 90天資料可能不完整，停止原因: {fetch_meta['stoppedReason']}")

            # 存入該粉專的個別數據
            final_data['allPages'].append({
                "name": page_name, "type": page_type, "followers": followers,
                "pageEngagement": page_engagement, "postEngagement": post_engagement,
                "postCount": valid_posts_count,
                "fetchMeta": fetch_meta
            })

            # 基準粉專深度處理
            if page_type == '基準粉專':
                base_recent_posts = list(posts)
                final_data['basePage'] = {
                    "name": page_name,
                    "followers": followers,
                    "pageEngagement": page_engagement,
                    "postEngagement": post_engagement,
                    "postCount": valid_posts_count,
                    "fetchMeta": fetch_meta
                }

            # 處理完一個粉專就存一次檔
            save_progress(final_data)

        except Exception as e:
            print(f"❌ 處理 {page_name} 時發生錯誤: {e}")
            traceback.print_exc()

    analysis_posts = base_recent_posts if base_recent_posts else []
    comment_texts = []
    comments_by_post_url = {}
    comment_target_posts = sorted(
        analysis_posts,
        key=lambda x: (calculate_comment_count(x), x.get('total_interactions', 0)),
        reverse=True
    )[:LLM_COMMENT_POST_LIMIT]

    for index, sp in enumerate(comment_target_posts):
        post_url = get_post_url(sp)
        if post_url:
            print(f"  └ 抓取留言數 Top {index+1} 貼文留言...")
            comments = fetch_post_comments(post_url)
            comments_by_post_url[post_url] = comments
            for c in comments:
                c_text = c.get('text') or c.get('message') or ''
                if c_text:
                    comment_texts.append(c_text)

    final_data['incompletePages'] = [
        {
            "name": p["name"],
            "type": p["type"],
            "postCount": p["postCount"],
            "stoppedReason": p.get("fetchMeta", {}).get("stoppedReason"),
            "oldestFetchedAt": p.get("fetchMeta", {}).get("oldestFetchedAt"),
            "newestFetchedAt": p.get("fetchMeta", {}).get("newestFetchedAt")
        }
        for p in final_data['allPages']
        if not p.get("fetchMeta", {}).get("isComplete90Days")
    ]
    final_data['contentAnalysis'] = summarize_content(analysis_posts, comment_texts)
    final_data['semanticAnalysis'] = run_llm_semantic_analysis(analysis_posts, comments_by_post_url)
    save_progress(final_data)

    print(f"\n✅ 所有程式執行完成，資料已成功儲存至 {OUTPUT_JSON_PATH}")

if __name__ == "__main__":
    main()
