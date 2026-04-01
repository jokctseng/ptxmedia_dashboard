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
import jieba

# ==========================================
# 1. 系統環境設定區
# ==========================================
BYCRAWL_API_KEY = os.environ.get('BYCRAWL_API_KEY', '您的_BYCRAWL_API_KEY')
OUTPUT_JSON_PATH = 'data.json'
HEADERS = {"x-api-key": BYCRAWL_API_KEY}

TW_TZ = timezone(timedelta(hours=8))

def safe_int(val):
    try: return int(val) if val is not None else 0
    except (ValueError, TypeError): return 0

# ==========================================
# 2. 核心抓取模組
# ==========================================
def extract_fb_id(url):
    if not url or not isinstance(url, str): return None
    url = url.split('?')[0].strip().rstrip('/')
    if 'profile.php' in url:
        parsed_url = urlparse(url)
        params = parse_qs(parsed_url.query)
        if 'id' in params: return params['id'][0]
    if '/p/' in url:
        match = re.search(r'-(\d+)$', url)
        if match: return match.group(1)
    parts = url.split('/')
    if len(parts) > 0: return parts[-1]
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
    if not username: return []
    all_posts = []
    cursor = None
    
    for _ in range(10):
        url = f"https://api.bycrawl.com/facebook/users/{username}/posts"
        params = {"count": 10} 
        if cursor: params["cursor"] = cursor
            
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=45)
            response.raise_for_status()
            res_json = response.json()
            
            if res_json.get("success") is False: break
            data_block = res_json.get("data", res_json)
            posts = data_block.get('posts', [])
            if not posts: break
            
            all_posts.extend(posts)
            cursor = data_block.get('nextCursor')
            if not cursor: break 
            
        except Exception as e:
            print(f"  [警告] 翻頁發生異常: {e}")
            break

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
    valid_posts = []
    
    for p in all_posts:
        created_at = p.get('createdAt') or p.get('created_at')
        if created_at and len(created_at) >= 19:
            try:
                post_dt = datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                if post_dt >= cutoff_date:
                    valid_posts.append(p)
            except:
                valid_posts.append(p) 
        else:
            valid_posts.append(p)
            
    return valid_posts

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
# 3. 數據分析與即時存檔模組
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

def main():
    print(f"啟動社群數據更新排程 (邊抓邊存進階版)... (執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    csv_file = get_csv_file()
    if not csv_file:
        print("❌ 找不到 .csv 檔案！")
        return

    
    final_data = {
        "lastUpdated": datetime.now(TW_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "dataScope": "最近 90 天活躍貼文",
        "basePage": {},
        "allPages": []
    }

    pages = []
    try:
        with open(csv_file, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader: pages.append(row)
    except Exception as e:
        print(f"❌ 讀取 CSV 發生錯誤: {e}")
        return

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
                continue
            
            followers = safe_int(profile_data.get('followerCount', profile_data.get('likesCount')))
            if followers == 0: 
                print("  [警告] 粉絲數為 0，跳過此粉專以防止計算錯誤。")
                continue
                
            posts = fetch_page_posts_90_days(username)
            total_interactions = 0
            valid_posts_count = len(posts)
            
            for p in posts:
                likes = safe_int(p.get('reactionCount') or p.get('likeCount') or p.get('likes'))
                comments = safe_int(p.get('commentCount') or p.get('comments'))
                shares = safe_int(p.get('shareCount') or p.get('shares'))
                
                interactions = likes + comments + shares
                total_interactions += interactions
                p['total_interactions'] = interactions
                
            if followers > 0 and valid_posts_count > 0:
                post_engagement = round((total_interactions / (followers * valid_posts_count)) * 100, 4)
                page_engagement = round((total_interactions / followers) * 100, 2)
            else:
                post_engagement = 0
                page_engagement = 0
            
            print(f"  └ 粉絲: {followers:,} | 90天內貼文: {valid_posts_count} | 總互動: {total_interactions} | 粉專互動率: {page_engagement}%")

            # 存入該粉專的個別數據
            final_data['allPages'].append({
                "name": page_name, "type": page_type, "followers": followers,
                "pageEngagement": page_engagement, "postEngagement": post_engagement
            })

            # 基準粉專深度處理
            if page_type == '基準粉專':
                all_comments_text = []
                post_texts = []
                hour_interactions = {}
                all_hashtags = []
                all_keywords = []
                
                weekday_map = {0: "週一", 1: "週二", 2: "週三", 3: "週四", 4: "週五", 5: "週六", 6: "週日"}
                stop_words = {'的', '是', '在', '了', '與', '和', '也', '有', '就', '我', '這', '都', '及', '為', '讓', '於', '以', '對', '我們', '大家', '一個', '可以', '不', '很', '會', '到', '上', '但', '那', '你', '他', '她'}

                sorted_posts = sorted(posts, key=lambda x: x.get('total_interactions', 0), reverse=True)
                top_posts_data = []

                for index, sp in enumerate(sorted_posts):
                    text = sp.get('text') or sp.get('message') or sp.get('description') or ''
                    if text: post_texts.append(text)

                    created_at = sp.get('createdAt') or sp.get('created_at')
                    interactions = sp.get('total_interactions', 0)
                    dt_tw = None
                    if created_at and len(created_at) >= 19:
                        try:
                            dt_utc = datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                            dt_tw = dt_utc.astimezone(TW_TZ)
                            key = f"{weekday_map[dt_tw.weekday()]} {dt_tw.hour:02d}:00"
                            hour_interactions[key] = hour_interactions.get(key, 0) + interactions
                        except Exception: pass

                    post_url = sp.get('permalinkUrl') or sp.get('url')
                    if index < 5 and post_url:
                        print(f"  └ 抓取 Top {index+1} 貼文留言...")
                        for c in fetch_post_comments(post_url):
                            c_text = c.get('text') or c.get('message') or ''
                            if c_text: all_comments_text.append(c_text)

                    if index < 20 and text:
                        hashtags = re.findall(r'#([^\s#]+)', text)
                        all_hashtags.extend(hashtags)
                        
                        text_clean = re.sub(r'#\S+', '', text)
                        words = jieba.cut(text_clean)
                        for w in words:
                            w = w.strip()
                            if len(w) > 1 and w not in stop_words and not w.encode().isalpha():
                                all_keywords.append(w)

                    if index < 20:
                        er_pct = round((interactions / followers) * 100, 2) if followers > 0 else 0
                        date_str = dt_tw.strftime("%Y-%m-%d") if dt_tw else (created_at[:10] if created_at else "未知")
                        top_posts_data.append({
                            "date": date_str,
                            "content": str(text)[:60] + '...' if text else "無文字內容",
                            "interactions": f"{interactions:,}",
                            "er": f"{er_pct}%"
                        })
                
                best_time_str = max(hour_interactions, key=hour_interactions.get) if hour_interactions else "資料不足"
                avg_daily = round(valid_posts_count / 90, 2)
                
                post_sentiment = analyze_sentiment(post_texts)
                comment_sentiment = analyze_sentiment(all_comments_text)
                
                top_hashtags = [{"tag": f"#{k}", "count": v} for k, v in collections.Counter(all_hashtags).most_common(10)]
                top_keywords = [{"word": k, "count": v} for k, v in collections.Counter(all_keywords).most_common(10)]

                final_data['basePage'] = {
                    "name": page_name, "followers": followers, "pageEngagement": page_engagement, "postEngagement": post_engagement,
                    "avgDailyPosts": avg_daily, "bestPostingTime": best_time_str,
                    "sentimentPostPositive": post_sentiment["positive"] if post_sentiment else None,
                    "sentimentPostNegative": post_sentiment["negative"] if post_sentiment else None,
                    "commentSentimentPositive": comment_sentiment["positive"] if comment_sentiment else None,
                    "commentSentimentNegative": comment_sentiment["negative"] if comment_sentiment else None,
                    "topHashtags": top_hashtags, "topKeywords": top_keywords,
                    "topPosts": top_posts_data # 將 topPosts 移入 basePage 物件內統整
                }

            # 處理完一個粉專就存一次檔
            save_progress(final_data)

        except Exception as e:
            print(f"❌ 處理 {page_name} 時發生錯誤: {e}")
            traceback.print_exc()

    print(f"\n✅ 所有程式執行完成，資料已成功儲存至 {OUTPUT_JSON_PATH}")

if __name__ == "__main__":
    main()
