📄 AdSurvivor: Meta Ads 自動化監控與優化系統
1. 專案概述 (Project Overview)
AdSurvivor 是一個基於 Python 與 Meta Graph API 開發的自動化廣告監控工具。專為管理 11 間香港醫美/美容分店（如 Natura Spa, Olase, LS Studio, Millie Beauty, Perfect M, Viva bella 等）所設計。
系統旨在解決多專頁管理困難、高 CPC（每次點擊成本）以及受眾定位偏差的問題，透過每日自動抓取數據並同步至 Google Sheets，實現數據可視化與自動化預警。

2. 核心策略與業務邏輯 (Business Logic)
低預算快閃測試：採用每日 $10 HKD 的小額測試策略，快速篩選出高潛力的廣告素材。

雙軌受眾策略 (The "Bun" Strategy)：

菲律賓受眾 (Bun)：藍海市場，互動率高，預期 CPC 極低 ($0.2 - $1.5)。

香港本地受眾 (HK)：競爭激烈，預期 CPC 較高 ($10 - $20+)，需嚴格控制止損。

零互動冷啟動優化：針對初期 (Like + Comment = 0) 的貼文，優化目標 (Optimization Goal) 從 POST_ENGAGEMENT 轉向 LINK_CLICKS，強迫演算法尋找點擊流量。

3. 系統架構與技術棧 (Tech Stack)
核心語言：Python 3.x (使用 asyncio 進行異步併發處理)。

網路請求：httpx (設定 timeout=30.0 處理大數據量回傳)。

API 串接：

Meta Graph API v18.0 (廣告數據提取)。

Google Sheets API & Google Drive API (使用 gspread 與 oauth2client 進行雲端同步)。

環境變數管理：python-dotenv (META_ACCESS_TOKEN, AD_ACCOUNT_ID)。

4. 核心功能模組 (Core Features)
4.1 數據精準提取 (Data Extraction)
全帳戶掃描：透過自動翻頁邏輯 (paging.next) 抓取單一廣告帳戶下所有運作中 (ACTIVE) 的廣告。

精準日結消耗：將 time_range 參數直接嵌入 insights 欄位中，精準抓取「昨天」與「今天」的單日消耗，解決預設抓取「全期/30天總額」的錯誤（12萬 HKD 錯誤）。

雙平台專頁識別 (Fallback Logic)：優先透過 actor_id 查詢 Facebook Page 名稱；若無，則自動回退查詢 instagram_actor_id 獲取 IG 帳號名稱。

4.2 深度定位與文案解析 (Targeting & Copy Analysis)
廣告文案 (Ad Body)：提取 creative.body 用於後續 NLP 關鍵字分析（如店鋪地點比對）。

目標設定解析 (Targeting Translation)：

地區 (Geo)：解析國家 (countries) 與精確城市/半徑 (cities)。

語言 (Languages)：將 ID 映射為可讀文字（如 6=英文, 24=繁體中文, 64=菲律賓語）。

興趣行為 (Flexible Spec)：提取受眾興趣（Interests）、行為與生活事件。

4.3 自動分類與標籤 (Auto-Categorization)
系統根據 廣告名稱 (Ad Name) 自動打標籤：

包含 BUN (不分大小寫) ➡️ 分類為 菲律賓(香港廣告)。

不包含 BUN ➡️ 分類為 Hong Kong。

4.4 雲端報表同步 (Google Sheets Sync)
每日自動清空並更新 Google Sheets (AdSurvivor_Report)。

按照「今日花費」降序排列，確保高消耗廣告置頂。

輸出欄位包含：來源專頁 | 分類 | 廣告名稱 | 昨 CPC | 今 CPC | 今花費 | 廣告文案 | 詳細目標設定。

底部自動加總計算昨日與今日的「總帳戶消耗」。

4.5 四區塊 Action Plan 工作表 (`AI_Action_Plan`，可透過 `.env` 的 `ACTION_PLAN_TAB` 覆寫名稱)
與既有 `AI_操作清單` 並存；由 `ai_optimizer.py` 寫入。同一分頁內以**欄 A 區塊標題**分隔，**僅輸出有資料的區塊**（無資料則整段不寫）。

1. **新建廣告清單**：P00（`pending_tests.json`，每店一列）＋ Explore/Exploit 的 `DUPLICATE_WITH_NEW_AUDIENCE`。新廣告名稱格式：`{店名}-{BUN|HK}-{縮短標題}`，Explore/Exploit 另加 `-e` / `-x` 後綴。
2. **暫停廣告清單**：`PAUSE_ONLY`；停預算回收欄為 `日預算 … | 今日 …`。**Next bot 僅執行暫停**，不含其他動作。
3. **預算調整清單**：與暫停清單 AdSet 互斥；建議與現有預算差異 ≥10% 才列出；調整原因僅標 **7日權重歸一化**；優先級固定 **P3**。不含月底預算保護、不含日內配速 80% 邏輯。
4. **受眾標籤置換清單**：Explore/Exploit 複製列（與新建清單對應）＋ `detect_fatigue` 之「素材衰退」且 7日/月均趨勢比 >1.25 之列；MAU 欄以 interest 驗證狀態摘要呈現。

5. 智能優化模組 (AdOptimizer Module - 已實作)
此模組為系統的「大腦」，負責在高 CPC 發生時介入分析。
AI 側寫策略 (Proxy Targeting)：
邏輯轉向：放棄競爭激烈的「醫學美容」、「抗衰老」等直接標籤，改用 「旁敲側擊 (Proxy)」 策略。
目標鎖定：鎖定高消費力女性的生活方式（如：Sephora, Yoga, Pilates, Fine dining, Wedding planning），以降低 CPM 並維持受眾品質。
AI 模型整合：支援 OpenAI 相容接口（如 Minimax abab6.5s），透過 temperature=0.3 確保輸出標籤的穩定性與純淨度（純英文、無括號）。
Meta API 受眾規模驗證 (Audience Size Validator)：
自動檢索：自動搜尋 AI 建議標籤的 interest_id。
MAU 門檻過濾：
🟢 適合擴量：香港預估受眾 (MAU) ≥ 200,000。
🔴 受眾太窄：MAU < 200,000（自動在報表中標記為淘汰）。
智能降級 (Smart Fallback)：若完整字串搜尋無果，自動拆解第一個單字或截取字首 4 碼進行模糊匹配，確保驗證流程不中斷。


6. 受眾物理隔離與防漏斗機制 (Audience Isolation)
針對香港特殊市場環境，實作硬性過濾邏輯，防止預算誤傷。
HK 本地隔離設定：
語言鎖定：強制包含 Chinese (Traditional)。
負向過濾 (Hard Exclude)：嚴格排除 Expats (Philippines)、Lived in Philippines 及語言 Tagalog。防止外籍傭工群體產生大量無效點擊。
菲律賓 (Bun) 隔離設定：
交集鎖定 (Narrow Audience)：受眾必須同時滿足 Expats (Philippines) 且 懂 English。
語言排除：嚴格排除 Chinese (Traditional)，防止誤觸用英文介面的香港本地高薪客群。
7. 系統穩定性與防崩潰機制 (Robustness & Error Handling)
針對 Meta API 的不穩定性與數據格式問題，實作了以下加固方案：
暴力清洗器 (Ultimate Cleaner)：在標籤送入 API 前，透過 Regex 強制移除所有中文字元、全半形括號、特殊符號，僅保留 a-z, A-Z, 0-9 與空格。
零推論防錯 (NoneType Defense)：加入 isinstance 檢查與 try-except 捕獲，防止 AI 回傳格式錯誤或 Meta API 報錯時（如 insufficient balance）導致整個異步迴圈崩潰。
語系自動匹配 (Locale Handling)：預設使用 en_US 進行標籤搜尋，解決 Meta API 在香港環境下強制轉中文導致標籤匹配失敗 (Mismatch) 的問題。
8. 數據字典與歷史記錄 (Targeting Dictionary)
分頁 B: 標籤成效資料庫：系統會自動紀錄所有測試過的標籤、對應的 MAU 規模以及當時觸發的 CPC。價值：隨時間積累，系統會自動生成一份「香港醫美精準受眾白皮書」，讓投手無需重複測試已知無效或受眾過窄的標籤。
9. 已解決之歷史技術問題 (Update)
 標籤找不到 (Mismatch)：取消名稱精確比對，改為只要 API 有回傳 ID 即視為有效。
 AI 輸出亂碼：透過 Prompt Engineering 強制純英文輸出，並降低模型溫度。
 語系限制報錯：移除 locale 參數，讓 API 自動適應帳戶語言環境。
10. 未來開發藍圖 (Updated Roadmap)
Phase 2: 門店地點聯動 (Geo-Fencing Logic)：自動解析文案中的分店地點（如：銅鑼灣），若定位半徑超過 5km 則自動報警。
Phase 3: 自動殺手 (The Kill Switch)：偵測到 CPC 異常飆升且消耗超過止損線時，自動暫停廣告。

### 修正：實作「月底預算保護 (EndOfMonth Protection)」
1. **取得數據**：計算 `current_day_of_month` 與 `total_days_in_month`。
2. **計算剩餘額度**：`Remaining_Budget = (Monthly_Budget - This_Month_Spend)`。
3. **計算安全日均**：`Safe_Daily_Limit = Remaining_Budget / (Total_Days - Current_Day + 1)`。
4. **決策干預**：
   - 如果 `今日建議預算` > `Safe_Daily_Limit * 1.2`：
     - 在原因欄位顯示： "⚠️ 月底預算超支風險：已自動下壓日預算建議至 ${Safe_Daily_Limit}"。
     - 強制將建議值下修，確保不破產。

     這份 requirement.md 是專為 11 間分店管理量身打造的「系統憲法」。它整合了 4 AM 執行邏輯、48 小時審核緩衝、以及穩定性優先的 7 天基準線。你可以直接將以下內容複製並存為 requirement.md，然後交給 Cursor 進行開發。📄 AdSurvivor: Meta Ads 智能決策與預算配速系統 (最終需求文檔)1. 專案概述 (Project Overview)AdSurvivor 是一個自動化監控與決策系統，旨在管理香港 11 間美容/醫美分店的 Meta 廣告。系統核心不再是單純的「開/關」廣告，而是作為一個 「智能配速員 (Pacer)」，透過 7 天基準線與每日預算微調，確保廣告在不頻繁驚動 Meta 演算法（Review Time）的情況下，達成預算目標。2. 廣告策略分類 (Strategy Hierarchy)系統需自動識別並標記廣告身分，套用不同的「寬容度」：[LTV] 高價值廣告：文案含 Gentlelase, 755, 12個月 等。價值高，波動寬容度高。[BUN] 菲律賓流量：名稱含 BUN。低成本來源，需嚴格控制在每日限額內。[NEW] 新帖孵化期：建立 < 72h 且消耗 < $150。受「免死金牌」保護，不建議改動。[GENERAL] 普通本地：一般的香港本地美容/療程廣告。3. 操作生命週期與時序 (Operational Lifecycle)為了對抗 Meta 審核機制 (Review Time) 並保護演算法穩定性，必須遵守以下時序邏輯：3.1 4 AM 重置原則 (The 4 AM Rule)重置時間：所有預算調整建議以 凌晨 4:00 AM 為基準。原因：Meta 預算以午夜重置，4 AM 調整能確保 Meta 有完整的 20 小時平穩分配新預算，避免傍晚加錢導致的 CPC 爆衝。3.2 48 小時審核緩衝 (The 48-Hour Buffer)發布策略：新素材建議提前 48 小時 上傳並設為「預約發布」。避免頻繁修改：系統優先建議「改預算」而非「改文案」，因為改文案會 100% 觸發 Review Time 並重置學習階段。4. 數據監控邏輯 (The Intelligence Engine)4.1 穩定性基準 (7-Day Baseline)核心決策依據：使用 last_7d_avg_cpc 作為判定廣告好壞的唯一指標。忽略單日波動：禁止僅因單日數據好壞而建議大幅改動。4.2 素材疲勞監測 (Fatigue Detection)當「今日 CPC」相對於「7天平均」出現異常跳動時觸發警報：LTV 廣告：今日 CPC > 7天平均 * 1.8 $\rightarrow$ 建議 📝 修改文案。一般廣告：今日 CPC > 7天平均 * 1.4 $\rightarrow$ 建議 📝 修改文案。4.3 預算配速與分流 (Budget Pacing)日預算制：將月預算拆解為 SHOP_DAILY_TOTAL。類別隔離：根據 BUN_RATIO 分割為 Bun 池與 HK 池。配速建議：今日花費已達 80% 但時間尚早 $\rightarrow$ 建議 🔽 下調明日日預算。7天平均表現優異且額度充足 $\rightarrow$ 建議 🔼 增加明日日預算。4.4 趨勢監控 (Trend Monitoring)每月平均趨勢：計算「本月至今平均 CPC」與「上月/過往 30 天平均」。功能：不直接干預操作，僅在 Sheets 顯示 🟢/⚠️/🔴 趨勢符號，用於判斷市場競爭是否整體上升。5. 系統架構與技術要求數據層 (callfrommeta.py)：並行抓取 today, 7d, 30d, this_month。執行店名映射 (SHOP_NAME_MAP)。決策層 (engine.py)：執行上述疲勞門檻與配速運算。顯示層 (ai_optimizer.py)：同步至 Google Sheets。6. .env 配置規格 (Full Flexibility)Code snippetSHOP_NAME_MAP='{...}'
SHOP_CONFIGS='{"旺角店": {"total": 830, "bun_ratio": 0.4}, ...}'
STRATEGY_TARGET_CPC='{"LTV": 35.0, "BUN": 1.5, "GENERAL": 18.0, "NEW": 20.0}'
LTV_VALUE_MULTIPLIER=2.0
FATIGUE_THRESHOLD_LTV=1.8
FATIGUE_THRESHOLD_GEN=1.4
LTV_KEYWORDS="Gentlelase,755,脫毛,12個月,永久"