import csv
import json
import os
import re
import requests
import collections
from datetime import datetime, timedelta, timezone
import traceback
from snownlp import SnowNLP
from urllib.parse import urlparse, parse_qs
# 導入中研院 CKIP Transformers
from ckip_transformers.nlp import CkipWordSegmenter

# ==========================================
# 1. 系統環境設定區
# ==========================================
BYCRAWL_API_KEY = os.environ.get('BYCRAWL_API_KEY', '您的_BYCRAWL_API_KEY')
CSV_FILE_PATH = '粉專資料追蹤清單.csv'
OUTPUT_JSON_PATH = 'data.json'
HEADERS = {"x-api-key": BYCRAWL_API_KEY}

# 設定台灣時區
TW_TZ = timezone(timedelta(hours=8))

# ==========================================
# 2. 核心抓取模組
# ==========================================
def extract_fb_id(url):
    """從 URL 萃取 Facebook username 或 ID"""
    if not url or not isinstance(url, str): return None
    url = url.strip().rstrip('/')
    if 'profile.php' in url:
        parsed_url = urlparse(url)
        params = parse_qs(parsed_url.query)
        if 'id' in params: return params['id'][0]
    if '/p/' in url:
        match = re.search(r'-(\d+)/?$', url)
        if match: return match.group(1)
    parts = url.split('/')
    if len(parts) > 3: return parts[-1].split('?')[0]
    return url

def fetch_page_profile(username):
    if not username: return None
    url = f"https://api.bycrawl.com/facebook/users/{username}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        res_json = response.json()
        if res_json.get("success") is False: return None
        return res_json.get("data", res_json)
    except Exception: return None

def fetch_page_posts_90_days(username):
    """取得最近 90 天的貼文，具備翻頁功能"""
    if not username: return []
    all_posts = []
    cursor = None
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
    
    # 最多翻 50 頁 
    for _ in range(50):
        url = f"https://api.bycrawl.com/facebook/users/{username}/posts"
        params = {"count": 10} # ByCrawl 單次最大數量
        if cursor: params["cursor"] = cursor
            
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=30)
            response.raise_for_status()
            res_json = response.json()
            
            if res_json.get("success") is False: break
            data_block = res_json.get("data", res_json)
            posts = data_block.get('posts', [])
            if not posts: break
            
            reached_cutoff = False
            for p in posts:
                created_at = p.get('createdAt') or p.get('created_at')
                if created_at:
                    try:
                        # 解析 UTC 時間
                        post_dt = datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                        if post_dt < cutoff_date:
                            reached_cutoff = True
                            break
                    except Exception: pass
                all_posts.append(p)
                
            if reached_cutoff: break
            
            cursor = data_block.get('nextCursor')
            if not cursor: break # 沒有下一頁了
            
        except Exception as e:
            print(f"  [警告] 翻頁抓取貼文發生異常: {e}")
            break
            
    return all_posts

def fetch_post_comments(post_url):
    if not post_url: return []
    url = "https://api.bycrawl.com/facebook/posts/comments"
    try:
        response = requests.get(url, params={"url": post_url}, headers=HEADERS, timeout=30)
        response.raise_for_status()
        res_json = response.json()
        if res_json.get("success") is False: return []
        data_block = res_json.get("data", res_json)
        return data_block.get('comments', [])
    except Exception: return []

# ==========================================
# 3. 數據分析模組
# ==========================================
def analyze_sentiment(text_list):
    if not text_list: return None
    positive_count = 0
    valid_text_count = 0
    for text in text_list:
        if not isinstance(text, str) or not text.strip(): continue
        try:
            if len(text.strip()) > 2:
                if SnowNLP(text).sentiments > 0.5: 
                    positive_count += 1
                valid_text_count += 1
        except Exception: pass
        
    if valid_text_count == 0: return None
    pos_ratio = round((positive_count / valid_text_count) * 100)
    return {"positive": pos_ratio, "negative": 100 - pos_ratio}

def main():
    print(f"啟動每月社群數據更新排程 (90天資料)... (執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    # 載入中研院 CKIP 輕量斷詞模型 
    print("載入中研院 CKIP 斷詞模型")
    ws_driver = CkipWordSegmenter(model="albert-tiny", device=-1)
    
    final_data = {
        "lastUpdated": datetime.now(TW_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "dataScope": "最近 90 天貼文",
        "basePage": {},
        "averages": {
            "all": {"name": "全部粉專平均", "pageEngagement": 0, "postEngagement": 0},
            "system": {"name": "林保署體系平均", "pageEngagement": 0, "postEngagement": 0},
            "subBranch": {"name": "屏東分署下轄平均", "pageEngagement": 0, "postEngagement": 0},
            "tourism": {"name": "屏東縣旅遊平均", "pageEngagement": 0, "postEngagement": 0}
        },
        "allPages": [],
        "matrix": [],
        "topPosts": []
    }

    category_stats = {
        "林保署體系": {"page_er_sum": 0, "post_er_sum": 0, "count": 0},
        "屏東分署下轄": {"page_er_sum": 0, "post_er_sum": 0, "count": 0},
        "屏東縣旅遊": {"page_er_sum": 0, "post_er_sum": 0, "count": 0},
        "all": {"page_er_sum": 0, "post_er_sum": 0, "count": 0}
    }

    pages = []
    if os.path.exists(CSV_FILE_PATH):
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader: pages.append(row)

    for page in pages:
        page_type = page.get('類型', '未知類型')
        page_name = page.get('粉專名稱', '未命名')
        username = extract_fb_id(page.get('網址', ''))
        if not username: continue
            
        print(f"\n🔄 正在處理: {page_name} (ID: {username})")
        profile_data = fetch_page_profile(username)
        if not profile_data: continue
        
        followers = profile_data.get('followerCount', profile_data.get('likesCount', 0))
        if followers == 0: continue
            
        # 抓取 90 天內的所有貼文
        posts = fetch_page_posts_90_days(username)
        total_interactions = 0
        valid_posts_count = len(posts)
        
        for p in posts:
            interactions = p.get('reactionCount', 0) + p.get('commentCount', 0) + p.get('shareCount', 0)
            total_interactions += interactions
            p['total_interactions'] = interactions
            
        if valid_posts_count == 0:
            post_engagement = 0; page_engagement = 0
        else:
            post_engagement = round((total_interactions / (followers * valid_posts_count)) * 100, 4)
            page_engagement = round((total_interactions / followers) * 100, 2)
        
        print(f"  └ 粉絲: {followers:,} | 90天內貼文: {valid_posts_count} | 粉專互動率: {page_engagement}%")

        final_data['allPages'].append({
            "name": page_name, "type": page_type, "followers": followers,
            "pageEngagement": page_engagement, "postEngagement": post_engagement
        })

        category_stats["all"]["page_er_sum"] += page_engagement
        category_stats["all"]["post_er_sum"] += post_engagement
        category_stats["all"]["count"] += 1
        if page_type in category_stats:
            category_stats[page_type]["page_er_sum"] += page_engagement
            category_stats[page_type]["post_er_sum"] += post_engagement
            category_stats[page_type]["count"] += 1

        final_data['matrix'].append({
            "name": page_name, "x": post_engagement * 100, "y": page_engagement,
            "color": '#10b981' if page_type == '基準粉專' else '#64748b' if page_type == '林保署體系' else '#f59e0b' if page_type == '屏東分署下轄' else '#3b82f6'
        })

        # ==========================================
        # 基準粉專專屬深度處理 (時區、CKIP 斷詞)
        # ==========================================
        if page_type == '基準粉專':
            all_comments_text = []
            post_texts = []
            hour_interactions = {}
            all_hashtags = []
            texts_for_ckip = [] # 準備送給 CKIP 的字串陣列
            
            stop_words = {'的', '是', '在', '了', '與', '和', '也', '有', '就', '我', '這', '都', '及', '為', '讓', '於', '以', '對', '我們', '大家'}

            sorted_posts = sorted(posts, key=lambda x: x.get('total_interactions', 0), reverse=True)
            
            for index, sp in enumerate(sorted_posts):
                text = sp.get('text') or sp.get('message') or sp.get('description') or ''
                if text: post_texts.append(text)

                # 時區轉換與最佳時間統計
                created_at = sp.get('createdAt') or sp.get('created_at')
                interactions = sp.get('total_interactions', 0)
                if created_at and len(created_at) >= 19:
                    try:
                        # 讀取 UTC 並轉台灣時間 (UTC+8)
                        dt_utc = datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                        dt_tw = dt_utc.astimezone(TW_TZ)
                        hour_tw = dt_tw.hour
                        hour_interactions[hour_tw] = hour_interactions.get(hour_tw, 0) + interactions
                    except Exception: pass

                # 抽取高互動貼文留言
                if index < 5 and sp.get('permalinkUrl'):
                    print(f"  └ 正在抓取最佳貼文留言進行 NLP 分析 ({index+1}/5)...")
                    for c in fetch_post_comments(sp['permalinkUrl']):
                        c_text = c.get('text') or c.get('message') or ''
                        if c_text: all_comments_text.append(c_text)

                # 針對 Top 20 的分析處理
                if index < 20:
                    if text:
                        texts_for_ckip.append(text)
                        hashtags = re.findall(r'#([^\s#]+)', text)
                        all_hashtags.extend(hashtags)

                    er_pct = round((interactions / followers) * 100, 2) if followers > 0 else 0
                    final_data['topPosts'].append({
                        "date": dt_tw.strftime("%Y-%m-%d") if created_at else "未知",
                        "content": str(text)[:60] + '...' if text else "無文字內容",
                        "interactions": f"{interactions:,}",
                        "er": f"{er_pct}%"
                    })
            
            # CKIP 中研院斷詞運算
            all_keywords = []
            if texts_for_ckip:
                print("  └ 執行 CKIP 中研院斷詞運算...")
                ws_results = ws_driver(texts_for_ckip)
                for ws_res in ws_results:
                    for w in ws_res:
                        w = w.strip()
                        if len(w) > 1 and w not in stop_words and not w.encode().isalpha():
                            all_keywords.append(w)

            best_hour = max(hour_interactions, key=hour_interactions.get) if hour_interactions else None
            best_time_str = f"{best_hour:02d}:00 - {(best_hour+1)%24:02d}:00" if best_hour is not None else None
            
            post_sentiment = analyze_sentiment(post_texts)
            comment_sentiment = analyze_sentiment(all_comments_text)
            
            top_hashtags = [{"tag": f"#{k}", "count": v} for k, v in collections.Counter(all_hashtags).most_common(10)]
            top_keywords = [{"word": k, "count": v} for k, v in collections.Counter(all_keywords).most_common(10)]

            final_data['basePage'] = {
                "name": page_name,
                "followers": followers,
                "pageEngagement": page_engagement,
                "postEngagement": post_engagement,
                "avgDailyPosts": round(valid_posts_count / 90, 1), # 改為 90 天平均
                "bestPostingTime": best_time_str,
                "sentimentPostPositive": post_sentiment["positive"] if post_sentiment else None,
                "sentimentPostNegative": post_sentiment["negative"] if post_sentiment else None,
                "commentSentimentPositive": comment_sentiment["positive"] if comment_sentiment else None,
                "commentSentimentNegative": comment_sentiment["negative"] if comment_sentiment else None,
                "topHashtags": top_hashtags,
                "topKeywords": top_keywords
            }

    # 計算分類平均
    if category_stats["all"]["count"] > 0:
        final_data["averages"]["all"]["pageEngagement"] = round(category_stats["all"]["page_er_sum"] / category_stats["all"]["count"], 2)
        final_data["averages"]["all"]["postEngagement"] = round(category_stats["all"]["post_er_sum"] / category_stats["all"]["count"], 4)
    
    cat_mapping = {"system": "林保署體系", "subBranch": "屏東分署下轄", "tourism": "屏東縣旅遊"}
    for eng_key, ch_key in cat_mapping.items():
        if category_stats[ch_key]["count"] > 0:
            final_data["averages"][eng_key]["pageEngagement"] = round(category_stats[ch_key]["page_er_sum"] / category_stats[ch_key]["count"], 2)
            final_data["averages"][eng_key]["postEngagement"] = round(category_stats[ch_key]["post_er_sum"] / category_stats[ch_key]["count"], 4)

    try:
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
        print(f"\n✅ 90 天資料更新完成！儲存至 {OUTPUT_JSON_PATH}")
    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    main()
