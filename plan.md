🚀 最終方案：AI 廣告生存者控制系統 (The Survivor System)
1. 核心控制塔 (Setting Page)
你會擁有一個簡單嘅 Web 介面，等你可以隨時調整：

分店管理：手動新增/刪除分店（不限 11 間，你可以隨時加減）。

網址綁定：每間店可以對應唔同嘅 Website URL 或 WhatsApp Link。

預算池 (Manual Overwrite)：手動輸入「香港池」同「菲律賓池」嘅總金額。

生存參數：手動設定 10 隻廣告上限、4-5 日觀察期、淘汰數量 等公式。

2. 「生存者」機制邏輯 (4-5 日循環)
Agent 會根據 「Post 發出時間」 作為計時起點：

第一階段 (Birth)：新 Post 出街，Agent 即時投放到對應池，初始預算由系統分配。

第二階段 (Learning)：廣告行足 4-5 日（你設定嘅時間），期間不作大改。

第三階段 (Ranking)：

Agent 抓取該分店/受眾池內所有運行中廣告的 CPC。

排序：由低至高排（最平嘅排第一）。

執行淘汰：如果廣告總數 > 10 隻，直接 Kill 掉排名最後（最貴）嗰幾隻，騰出預算空間畀聽日嘅新 Post。

3. Google Ads 靈活性
你可以喺 Setting Page 設定 3 個（或更多）不同的網站網址。

Agent 喺建立 Google Search Ads 時，會自動跟返你 Assigned 俾嗰間分店嘅 URL。

AI 會自動根據 FB Post 文案生成 Search Keyword，確保 Google 同 FB 嘅 Promo 內容係同步嘅。

📊 系統數據流向圖
🛠️ 給 Cursor 的技術開發架構 (Roadmap)
如果你準備開工，我建議按照以下模組叫 Cursor 寫 code：

第一步：Database & Config Schema (資料庫設計)
你需要一張 branches 表同埋一格 global_settings 表。

Branches: id, name, fb_page_id, whatsapp_link, website_url, audience_type (HK/PH).

Ads_Tracking: ad_id, post_id, created_at, current_cpc, status (Active/Killed).

第二步：The "Post Monitor" (監控器)
寫一個腳本，每 6 小時 Check 一次 11 個 Page：

如果發現新 post_id 唔喺資料庫，即刻建立廣告。

記錄 start_date = post_publish_date。

第三步：The "Survivor Engine" (淘汰引擎)
寫一個 Cron Job（每日行一次）：

Python
# 邏輯偽代碼
if current_date - post_publish_date >= 5_days:
    all_ads = get_ads_by_pool(branch_id)
    if len(all_ads) > 10:
        worst_ads = sort_by_cpc(all_ads).last(2)
        stop_ads(worst_ads)
第四步：The Dashboard (控制面板)
用 Next.js + Tailwind CSS 整一個 Table，俾你改：

HK_Budget_Pool: $13,000

PH_Budget_Pool: $5,000

Survivor_Threshold_Days: 5