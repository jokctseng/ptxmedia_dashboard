import csv
import json
import os
import re
import requests
from datetime import datetime
import traceback
from snownlp import SnowNLP
from urllib.parse import urlparse, parse_qs

# ==========================================
# 1. 系統環境設定區
# ==========================================
BYCRAWL_API_KEY = os.environ.get('BYCRAWL_API_KEY', '您的_BYCRAWL_API_KEY')
CSV_FILE_PATH = '粉專資料追蹤清單.csv'
OUTPUT_JSON_PATH = 'data.json'
HEADERS = {"x-api-key": BYCRAWL_API_KEY}

# ==========================================
# 2. 核心抓取模組 
# ==========================================
def extract_fb_id(url):
    """
    從各種 Facebook 網址格式中萃取正確的 ID 或 Username。
    支援：
    1. 一般 Username (例: Pingtung.Branch.FANCA)
    2. /p/ 中文名稱-數字ID (例: 林業署臺中分署-100064182631791 -> 取出 100064182631791)
    3. profile.php?id=數字ID
    """
    if not url or not isinstance(url, str): return None
    url = url.strip().rstrip('/')
    
    # 處理 profile.php?id= 格式
    if 'profile.php' in url:
        parsed_url = urlparse(url)
        params = parse_qs(parsed_url.query)
        if 'id' in params: return params['id'][0]
            
    # 處理 /p/ 名稱-數字ID 格式
    if '/p/' in url:
        match = re.search(r'-(\d+)/?$', url)
        if match: return match.group(1)
            
    # 處理一般的 username 格式
    parts = url.split('/')
    if len(parts) > 3:
        username = parts[-1].split('?')[0]
        return username
    
    return url

def fetch_page_profile(username):
    if not username: return None
    url = f"https://api.bycrawl.com/facebook/users/{username}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        res_json = response.json()
        if res_json.get("success") is False:
            print(f"  [警告] API 取得粉專資訊失敗 ({username}): {res_json.get('error')}")
            return None
        return res_json.get("data", res_json)
    except Exception as e:
        print(f"  [錯誤] 取得粉專資訊異常 ({username}): {e}")
        return None

def fetch_page_posts(username, count=10):
    if not username: return []
    url = f"https://api.bycrawl.com/facebook/users/{username}/posts"
    try:
        response = requests.get(url, params={"count": count}, headers=HEADERS, timeout=30)
        response.raise_for_status()
        res_json = response.json()
        if res_json.get("success") is False:
            print(f"  [警告] API 取得貼文失敗 ({username}): {res_json.get('error')}")
            return []
        data_block = res_json.get("data", res_json)
        return data_block.get('posts', [])
    except Exception as e:
        print(f"  [錯誤] 取得貼文異常 ({username}): {e}")
        return []

def fetch_post_comments(post_url):
    if not post_url: return []
    url = "https://api.bycrawl.com/facebook/posts/comments"
    try:
        response = requests.get(url, params={"url": post_url}, headers=HEADERS, timeout=30)
        response.raise_for_status()
        res_json = response.json()
        if res_json.get("success") is False:
            return []
        data_block = res_json.get("data", res_json)
        return data_block.get('comments', [])
    except Exception as e:
        print(f"  [錯誤] 取得留言異常 ({post_url}): {e}")
        return []

# ==========================================
# 3. 數據分析模組 (NLP & 計算)
# ==========================================
def analyze_sentiment(text_list):
    if not text_list: return None
    positive_count = 0
    valid_text_count = 0
    for text in text_list:
        if not isinstance(text, str) or not text.strip(): continue
        try:
            if len(text.strip()) > 2: # 僅判斷有意義長度的文字
                if SnowNLP(text).sentiments > 0.5: 
                    positive_count += 1
                valid_text_count += 1
        except Exception: pass
        
    if valid_text_count == 0: return None
    pos_ratio = round((positive_count / valid_text_count) * 100)
    return {"positive": pos_ratio, "negative": 100 - pos_ratio}

def main():
    print(f"啟動每月社群數據更新排程... (執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    final_data = {
        "lastUpdated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ 找不到 CSV 檔案: {CSV_FILE_PATH}")
        return

    with open(CSV_FILE_PATH, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pages.append(row)

    print(f"📝 成功讀取 {len(pages)} 個粉專準備抓取。")

    for page in pages:
        page_type = page.get('類型', '未知類型')
        page_name = page.get('粉專名稱', '未命名')
        page_url = page.get('網址', '')
        
        if not page_url: continue
            
        username = extract_fb_id(page_url)
        print(f"\n🔄 正在處理: {page_name} (解析 ID: {username})")
        
        if not username:
            print(f"  [跳過] 無法解析網址: {page_url}")
            continue
        
        # 1. 取粉專資訊
        profile_data = fetch_page_profile(username)
        if not profile_data: continue
        
        followers = profile_data.get('followerCount', profile_data.get('likesCount', 0))
        if followers == 0:
            print(f"  [注意] 粉絲數為 0，為避免除以零將跳過互動率計算。")
            continue
            
        # 2. 取貼文資訊
        posts = fetch_page_posts(username, count=10)
        
        total_interactions = 0
        valid_posts_count = 0
        
        for p in posts:
            interactions = p.get('reactionCount', 0) + p.get('commentCount', 0) + p.get('shareCount', 0)
            total_interactions += interactions
            p['total_interactions'] = interactions
            valid_posts_count += 1
            
        if valid_posts_count == 0:
            print(f"  [注意] 取得 0 篇貼文，互動率記為 0。")
            post_engagement = 0
            page_engagement = 0
        else:
            post_engagement = round((total_interactions / (followers * valid_posts_count)) * 100, 4)
            page_engagement = round((total_interactions / followers) * 100, 2)
        
        print(f"  └ 粉絲數: {followers:,} | 近期貼文: {valid_posts_count} | 總互動: {total_interactions} | 粉專互動率: {page_engagement}%")

        # 寫入清單供前端表格顯示
        final_data['allPages'].append({
            "name": page_name,
            "type": page_type,
            "followers": followers,
            "pageEngagement": page_engagement,
            "postEngagement": post_engagement
        })

        # 累加平均值
        category_stats["all"]["page_er_sum"] += page_engagement
        category_stats["all"]["post_er_sum"] += post_engagement
        category_stats["all"]["count"] += 1
        if page_type in category_stats:
            category_stats[page_type]["page_er_sum"] += page_engagement
            category_stats[page_type]["post_er_sum"] += post_engagement
            category_stats[page_type]["count"] += 1

        # 散佈圖資料 (X: 貼文互動率, Y: 粉專互動率) -> 無任何模擬數據
        final_data['matrix'].append({
            "name": page_name,
            "x": post_engagement * 100, # 放大比例以利繪圖呈現
            "y": page_engagement,
            "color": '#10b981' if page_type == '基準粉專' else '#64748b' if page_type == '林保署體系' else '#f59e0b' if page_type == '屏東分署下轄' else '#3b82f6'
        })

        # 基準粉專深度分析
        if page_type == '基準粉專':
            all_comments_text = []
            post_texts = []
            hour_interactions = {}
            sorted_posts = sorted(posts, key=lambda x: x.get('total_interactions', 0), reverse=True)
            
            for index, sp in enumerate(sorted_posts):
                text = sp.get('text', '')
                if text: post_texts.append(text)

                created_at = sp.get('createdAt', '')
                interactions = sp.get('total_interactions', 0)
                if created_at:
                    try:
                        dt = datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S")
                        hour = dt.hour
                        hour_interactions[hour] = hour_interactions.get(hour, 0) + interactions
                    except Exception: pass

                if index < 5 and sp.get('permalinkUrl'):
                    print(f"  └ 正在抓取最佳貼文留言進行 NLP 分析 ({index+1}/5)...")
                    comments_data = fetch_post_comments(sp['permalinkUrl'])
                    for c in comments_data:
                        c_text = c.get('text', '')
                        if c_text: all_comments_text.append(c_text)

                if index < 20:
                    er_pct = round((interactions / followers) * 100, 2) if followers > 0 else 0
                    raw_date = sp.get('createdAt', '')
                    final_data['topPosts'].append({
                        "date": raw_date[:10] if raw_date else "未知",
                        "content": str(text)[:60] + '...' if text else "無文字內容",
                        "interactions": f"{interactions:,}",
                        "er": f"{er_pct}%"
                    })
            
            best_hour = max(hour_interactions, key=hour_interactions.get) if hour_interactions else None
            best_time_str = f"{best_hour:02d}:00 - {(best_hour+1)%24:02d}:00" if best_hour is not None else None
            
            post_sentiment = analyze_sentiment(post_texts)
            comment_sentiment = analyze_sentiment(all_comments_text)
            
            final_data['basePage'] = {
                "name": page_name,
                "followers": followers,
                "pageEngagement": page_engagement,
                "postEngagement": post_engagement,
                "avgDailyPosts": round(valid_posts_count / 30, 1) if valid_posts_count > 0 else 0,
                "bestPostingTime": best_time_str,
                "sentimentPostPositive": post_sentiment["positive"] if post_sentiment else None,
                "sentimentPostNegative": post_sentiment["negative"] if post_sentiment else None,
                "commentSentimentPositive": comment_sentiment["positive"] if comment_sentiment else None,
                "commentSentimentNegative": comment_sentiment["negative"] if comment_sentiment else None
            }

    # 4. 計算分類平均
    if category_stats["all"]["count"] > 0:
        final_data["averages"]["all"]["pageEngagement"] = round(category_stats["all"]["page_er_sum"] / category_stats["all"]["count"], 2)
        final_data["averages"]["all"]["postEngagement"] = round(category_stats["all"]["post_er_sum"] / category_stats["all"]["count"], 4)
    
    cat_mapping = {"system": "林保署體系", "subBranch": "屏東分署下轄", "tourism": "屏東縣旅遊"}
    for eng_key, ch_key in cat_mapping.items():
        if category_stats[ch_key]["count"] > 0:
            final_data["averages"][eng_key]["pageEngagement"] = round(category_stats[ch_key]["page_er_sum"] / category_stats[ch_key]["count"], 2)
            final_data["averages"][eng_key]["postEngagement"] = round(category_stats[ch_key]["post_er_sum"] / category_stats[ch_key]["count"], 4)

    # 匯出資料
    try:
        with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
        print(f"\n✅ 資料更新完成！總共成功處理 {category_stats['all']['count']} 個粉專，結果已儲存至 {OUTPUT_JSON_PATH}")
    except Exception as e:
        print(f"❌ 儲存 JSON 發生錯誤: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
