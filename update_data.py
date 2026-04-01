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

# 設定台灣時區
TW_TZ = timezone(timedelta(hours=8))

def safe_int(val):
    """安全地將各種型態轉換為整數，避免字串或 None 造成錯誤"""
    try:
        return int(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0

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
    except Exception as e: 
        print(f"  [錯誤] fetch_page_profile: {e}")
        return None

def fetch_page_posts_90_days(username):
    """取得最近 90 天的貼文 (以粉專最新貼文為基準日往前推算)"""
    if not username: return []
    all_posts = []
    cursor = None
    
    # 抓取最多 10 頁 (約 100 篇貼文)
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
            print(f"  [警告] 翻頁抓取貼文發生異常: {e}")
            break
            
    if not all_posts:
        return []

    # 找出該粉專最新的一篇貼文日期，當作 90 天的計算基準點，避免舊粉專全部歸零
    latest_date = None
    for p in all_posts:
        created_at = p.get('createdAt') or p.get('created_at')
        if created_at and len(created_at) >= 19:
            try:
                dt = datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                if not latest_date or dt > latest_date:
                    latest_date = dt
            except: pass
            
    if not latest_date:
        latest_date = datetime.now(timezone.utc)
        
    cutoff_date = latest_date - timedelta(days=90)
    
    # 篩選出該粉專活躍區間 90 天內的貼文 (可過濾掉極老的置頂文)
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
            
    # 如果過濾完還是 0 篇，至少給最近 10 篇確保有數據可算
    if not valid_posts and all_posts:
        valid_posts = all_posts[:10]
        
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
    """自動尋找當前目錄下的 csv 檔案"""
    for file in os.listdir('.'):
        if file.endswith('.csv'):
            return file
    return None

def main():
    print(f"啟動每月社群數據更新排程 (90天 & Jieba)... (執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    csv_file = get_csv_file()
    if not csv_file:
        print("❌ 找不到任何 .csv 檔案，請確認檔案已上傳！")
        return
    print(f"找到 CSV 檔案: {csv_file}")

    final_data = {
        "lastUpdated": datetime.now(TW_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "dataScope": "最新 90 天活躍貼文",
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
            page_type = page.get('類型', '未知類型')
            page_name = page.get('粉專名稱', '未命名')
            username = extract_fb_id(page.get('網址', ''))
            
            if not username: continue
                
            print(f"\n🔄 正在處理: {page_name} (ID: {username})")
            profile_data = fetch_page_profile(username)
            if not profile_data: 
                print("  └ 無法取得 profile_data，跳過此粉專。")
                continue
            
            # 使用 safe_int 確保取得數字
            followers = safe_int(profile_data.get('followerCount', profile_data.get('likesCount')))
            if followers == 0: 
                print("  └ 粉絲數為 0，跳過。")
                continue
                
            posts = fetch_page_posts_90_days(username)
            total_interactions = 0
            valid_posts_count = len(posts)
            
            for p in posts:
                # 兼容多種 API 回傳的互動數名稱，並安全轉為整數
                likes = safe_int(p.get('reactionCount') or p.get('likeCount') or p.get('likes'))
                comments = safe_int(p.get('commentCount') or p.get('comments'))
                shares = safe_int(p.get('shareCount') or p.get('shares'))
                
                interactions = likes + comments + shares
                total_interactions += interactions
                p['total_interactions'] = interactions
                
            if valid_posts_count == 0:
                post_engagement = 0; page_engagement = 0
            else:
                post_engagement = round((total_interactions / (followers * valid_posts_count)) * 100, 4)
                page_engagement = round((total_interactions / followers) * 100, 2)
            
            print(f"  └ 粉絲: {followers:,} | 有效貼文: {valid_posts_count} | 總互動: {total_interactions} | 粉專互動率: {page_engagement}%")

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
            # 基準粉專專屬深度處理 (最佳時間、關鍵字、留言)
            # ==========================================
            if page_type == '基準粉專':
                all_comments_text = []
                post_texts = []
                hour_interactions = {}
                all_hashtags = []
                all_keywords = []
                
                weekday_map = {0: "週一", 1: "週二", 2: "週三", 3: "週四", 4: "週五", 5: "週六", 6: "週日"}
                stop_words = {'的', '是', '在', '了', '與', '和', '也', '有', '就', '我', '這', '都', '及', '為', '讓', '於', '以', '對', '我們', '大家', '一個', '可以', '不', '很', '會', '到', '上', '但', '這', '那', '你', '他', '她'}

                sorted_posts = sorted(posts, key=lambda x: x.get('total_interactions', 0), reverse=True)
                
                for index, sp in enumerate(sorted_posts):
                    text = sp.get('text') or sp.get('message') or sp.get('description') or ''
                    if text: post_texts.append(text)

                    created_at = sp.get('createdAt') or sp.get('created_at')
                    interactions = sp.get('total_interactions', 0)
                    dt_tw = None
                    if created_at and len(created_at) >= 19:
                        try:
                            # 轉換為台灣時間，並記錄 星期幾 + 幾點
                            dt_utc = datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                            dt_tw = dt_utc.astimezone(TW_TZ)
                            
                            key = f"{weekday_map[dt_tw.weekday()]} {dt_tw.hour:02d}:00"
                            hour_interactions[key] = hour_interactions.get(key, 0) + interactions
                        except Exception: pass

                    # 雙重相容 URL 欄位 (解決抓不到留言的問題)
                    post_url = sp.get('permalinkUrl') or sp.get('url')
                    
                    if index < 5 and post_url:
                        print(f"  └ 正在抓取貼文留言進行分析 ({index+1}/5)...")
                        for c in fetch_post_comments(post_url):
                            c_text = c.get('text') or c.get('message') or ''
                            if c_text: all_comments_text.append(c_text)

                    if index < 20:
                        if text:
                            # 萃取標籤
                            hashtags = re.findall(r'#([^\s#]+)', text)
                            all_hashtags.extend(hashtags)
                            
                            # 去除標籤後，再進行關鍵字斷詞
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
                
                # 計算最佳時間 (總互動數最高的 星期X 幾點)
                best_time_str = max(hour_interactions, key=hour_interactions.get) if hour_interactions else "資料不足"
                
                post_sentiment = analyze_sentiment(post_texts)
                comment_sentiment = analyze_sentiment(all_comments_text)
                
                top_hashtags = [{"tag": f"#{k}", "count": v} for k, v in collections.Counter(all_hashtags).most_common(10)]
                top_keywords = [{"word": k, "count": v} for k, v in collections.Counter(all_keywords).most_common(10)]

                final_data['basePage'] = {
                    "name": page_name,
                    "followers": followers,
                    "pageEngagement": page_engagement,
                    "postEngagement": post_engagement,
                    "avgDailyPosts": round(valid_posts_count / 90, 1) if valid_posts_count else 0,
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
        print(f"❌ 儲存 JSON 發生錯誤: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
