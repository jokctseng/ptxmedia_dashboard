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

# 設定臺灣時區
TW_TZ = timezone(timedelta(hours=8))

def safe_int(val):
    try: return int(val) if val is not None else 0
    except (ValueError, TypeError): return 0

# ==========================================
# 2. 核心抓取模組
# ==========================================
def extract_fb_id(url):
    """強化版 URL 萃取，排除隱形參數干擾"""
    if not url or not isinstance(url, str): return None
    
    # 移除任何 URL 參數 (如 ?mibextid=xxx)
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

def fetch_recent_posts(username):
    """ByCrawl API 限制單次最多回傳 10 篇貼文"""
    if not username: return []
    url = f"https://api.bycrawl.com/facebook/users/{username}/posts"
    try:
        response = requests.get(url, params={"count": 10}, headers=HEADERS, timeout=45)
        response.raise_for_status()
        res_json = response.json()
        if res_json.get("success") is False: return []
        data_block = res_json.get("data", res_json)
        return data_block.get('posts', [])
    except Exception as e:
        print(f"  [警告] 取得貼文發生異常: {e}")
        return []

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

def get_csv_file():
    for file in os.listdir('.'):
        if file.endswith('.csv'): return file
    return None

def main():
    print(f"啟動社群數據更新排程... (執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    csv_file = get_csv_file()
    if not csv_file:
        print("❌ 找不到 .csv 檔案！")
        return

    final_data = {
        "lastUpdated": datetime.now(TW_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "dataScope": "近期最新 10 篇貼文",
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
    try:
        with open(csv_file, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader: pages.append(row)
    except Exception as e:
        print(f"❌ 讀取 CSV 發生錯誤: {e}")
        return

    for page in pages:
        try:
            # 加入 .strip() 清除 CSV 中可能殘留的空白，避免前端分類對不上
            page_type = str(page.get('類型', '未知類型')).strip()
            page_name = str(page.get('粉專名稱', '未命名')).strip()
            page_url = str(page.get('網址', '')).strip()
            
            if not page_url: continue
                
            username = extract_fb_id(page_url)
            print(f"\n🔄 處理中: {page_name} (類型: {page_type} | ID: {username})")
            
            profile_data = fetch_page_profile(username)
            if not profile_data: 
                print("  [警告] 取無 profile_data，但仍會保留在名單中。")
                profile_data = {}
            
            fc = profile_data.get('followerCount')
            lc = profile_data.get('likesCount')
            followers = safe_int(fc) if fc is not None else safe_int(lc)
            
            posts = fetch_recent_posts(username)
            total_interactions = 0
            valid_posts_count = len(posts)
            
            for p in posts:
                likes = safe_int(p.get('reactionCount') or p.get('likeCount') or p.get('likes'))
                comments = safe_int(p.get('commentCount') or p.get('comments'))
                shares = safe_int(p.get('shareCount') or p.get('shares'))
                
                interactions = likes + comments + shares
                total_interactions += interactions
                p['total_interactions'] = interactions
                
            # 互動率計算
            if followers > 0 and valid_posts_count > 0:
                post_engagement = round((total_interactions / (followers * valid_posts_count)) * 100, 4)
                page_engagement = round((total_interactions / followers) * 100, 2)
            else:
                post_engagement = 0
                page_engagement = 0
            
            print(f"  └ 粉絲: {followers:,} | 取得貼文: {valid_posts_count} | 總互動: {total_interactions} | 粉專互動率: {page_engagement}%")

            # 只要有在 CSV 中，就一定加進 allPages
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
            # 基準粉專專屬深度處理
            # ==========================================
            if page_type == '基準粉專':
                all_comments_text = []
                post_texts = []
                hour_interactions = {}
                all_hashtags = []
                all_keywords = []
                post_dates = []
                
                weekday_map = {0: "週一", 1: "週二", 2: "週三", 3: "週四", 4: "週五", 5: "週六", 6: "週日"}
                stop_words = {'的', '是', '在', '了', '與', '和', '也', '有', '就', '我', '這', '都', '及', '為', '讓', '於', '以', '對', '我們', '大家', '一個', '可以', '不', '很', '會', '到', '上', '但', '那', '你', '他', '她'}

                sorted_posts = sorted(posts, key=lambda x: x.get('total_interactions', 0), reverse=True)
                
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
                            
                            # 記錄該篇貼文的日期，供計算「平均每日發文」
                            post_dates.append(dt_tw.date())
                            
                            key = f"{weekday_map[dt_tw.weekday()]} {dt_tw.hour:02d}:00"
                            hour_interactions[key] = hour_interactions.get(key, 0) + interactions
                        except Exception: pass

                    post_url = sp.get('permalinkUrl') or sp.get('url')
                    
                    if index < 5 and post_url:
                        print(f"  └ 正在抓取貼文留言進行分析 ({index+1}/5)...")
                        for c in fetch_post_comments(post_url):
                            c_text = c.get('text') or c.get('message') or ''
                            if c_text: all_comments_text.append(c_text)

                    if text:
                        hashtags = re.findall(r'#([^\s#]+)', text)
                        all_hashtags.extend(hashtags)
                        
                        text_clean = re.sub(r'#\S+', '', text)
                        words = jieba.cut(text_clean)
                        for w in words:
                            w = w.strip()
                            if len(w) > 1 and w not in stop_words and not w.encode().isalpha():
                                all_keywords.append(w)

                    er_pct = round((interactions / followers) * 100, 2) if followers > 0 else 0
                    date_str = dt_tw.strftime("%Y-%m-%d") if dt_tw else (created_at[:10] if created_at else "未知")
                    final_data['topPosts'].append({
                        "date": date_str,
                        "content": str(text)[:60] + '...' if text else "無文字內容",
                        "interactions": f"{interactions:,}",
                        "er": f"{er_pct}%"
                    })
                
                best_time_str = max(hour_interactions, key=hour_interactions.get) if hour_interactions else "資料不足"
                
                # 精確計算：以這 10 篇貼文橫跨的「實際天數」來算每日平均發文
                avg_daily = 0
                if len(post_dates) > 1:
                    delta_days = (max(post_dates) - min(post_dates)).days
                    delta_days = max(1, delta_days) # 防止除以 0
                    avg_daily = round(valid_posts_count / delta_days, 2)
                elif valid_posts_count == 1:
                    avg_daily = 1
                
                post_sentiment = analyze_sentiment(post_texts)
                comment_sentiment = analyze_sentiment(all_comments_text)
                
                top_hashtags = [{"tag": f"#{k}", "count": v} for k, v in collections.Counter(all_hashtags).most_common(10)]
                top_keywords = [{"word": k, "count": v} for k, v in collections.Counter(all_keywords).most_common(10)]

                final_data['basePage'] = {
                    "name": page_name,
                    "followers": followers,
                    "pageEngagement": page_engagement,
                    "postEngagement": post_engagement,
                    "avgDailyPosts": avg_daily,
                    "bestPostingTime": best_time_str,
                    "sentimentPostPositive": post_sentiment["positive"] if post_sentiment else None,
                    "sentimentPostNegative": post_sentiment["negative"] if post_sentiment else None,
                    "commentSentimentPositive": comment_sentiment["positive"] if comment_sentiment else None,
                    "commentSentimentNegative": comment_sentiment["negative"] if comment_sentiment else None,
                    "topHashtags": top_hashtags,
                    "topKeywords": top_keywords
                }

        except Exception as e:
            print(f"❌ 處理 {page_name} 時發生非預期錯誤: {e}")
            traceback.print_exc()

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
        print(f"\n✅ 資料更新完成！成功儲存至 {OUTPUT_JSON_PATH}")
    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    main()
