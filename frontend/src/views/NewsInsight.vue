<template>
  <div class="news-wrap">

    <!-- ① 顶栏：三大 AI Function 操作区 -->
    <div class="ai-bar card">
      <div class="ai-bar-left">
        <div class="ai-bar-title">
          <span class="doris-badge">⚡ Doris 4.0</span>
          基金资讯 AI Function 分析平台
        </div>
        <div class="ai-bar-sub">AI_SUMMARIZE · AI_SENTIMENT · AI_EXTRACT — 文本语义分析在 Doris 引擎内完成，数据不出库</div>
      </div>
      <div class="ai-actions">
        <el-button type="info" :loading="importing" @click="doImport" :disabled="stats.total>0">
          📥 导入资讯 (25条)
        </el-button>
        <div class="ai-btn-group">
          <el-button class="ai-btn summarize" :loading="running==='summarize'" :disabled="!canRun"
            @click="runAI('summarize')">
            <span class="fn-name">AI_SUMMARIZE</span>
            <span class="fn-desc">文本概括</span>
          </el-button>
          <el-button class="ai-btn sentiment" :loading="running==='sentiment'" :disabled="!canRun"
            @click="runAI('sentiment')">
            <span class="fn-name">AI_SENTIMENT</span>
            <span class="fn-desc">情感分析</span>
          </el-button>
          <el-button class="ai-btn extract" :loading="running==='extract'" :disabled="!canRun"
            @click="runAI('extract')">
            <span class="fn-name">AI_EXTRACT</span>
            <span class="fn-desc">标签提取</span>
          </el-button>
        </div>
      </div>
    </div>

    <!-- ② KPI 进度条 -->
    <div class="progress-bar card" v-if="stats.total>0">
      <div class="prog-item">
        <div class="prog-num">{{ stats.total }}</div>
        <div class="prog-label">资讯总量</div>
      </div>
      <div class="prog-divider"></div>
      <div class="prog-item">
        <div class="prog-num blue">{{ stats.summarized }}</div>
        <div class="prog-label">已概括</div>
        <el-progress :percentage="pct(stats.summarized, stats.total)" color="#409eff" :stroke-width="4" :show-text="false" style="width:80px;margin-top:4px"/>
      </div>
      <div class="prog-divider"></div>
      <div class="prog-item">
        <div class="prog-num green">{{ stats.sentiment_done }}</div>
        <div class="prog-label">已情感分析</div>
        <el-progress :percentage="pct(stats.sentiment_done, stats.total)" color="#67c23a" :stroke-width="4" :show-text="false" style="width:80px;margin-top:4px"/>
      </div>
      <div class="prog-divider"></div>
      <div class="prog-item">
        <div class="prog-num orange">{{ stats.extracted }}</div>
        <div class="prog-label">已提取标签</div>
        <el-progress :percentage="pct(stats.extracted, stats.total)" color="#e6a23c" :stroke-width="4" :show-text="false" style="width:80px;margin-top:4px"/>
      </div>
      <div class="prog-divider"></div>
      <div class="prog-item" v-if="lastSql">
        <el-button size="small" type="info" plain @click="sqlVisible=true">查看执行 SQL</el-button>
      </div>
    </div>

    <!-- SQL 弹窗 -->
    <el-dialog v-model="sqlVisible" title="Doris AI Function SQL" width="700px">
      <pre class="sql-dialog-code">{{ lastSql }}</pre>
      <div class="sql-dialog-tip">⚡ 该 SQL 在 Doris 引擎内部直接调用 AI Function，数据无需离开数据库</div>
    </el-dialog>

    <!-- ③ 主区域：左列表 + 右详情 -->
    <div class="main-area" v-if="stats.total>0">

      <!-- 左：资讯列表 -->
      <div class="list-panel">
        <div class="list-hd">
          <el-input v-model="keyword" placeholder="搜索关键词…" size="small" clearable style="flex:1" @change="loadList" />
          <el-select v-model="filterSector" size="small" placeholder="板块" clearable style="width:82px" @change="loadList">
            <el-option v-for="s in sectors" :key="s" :label="s" :value="s"/>
          </el-select>
          <el-select v-model="filterSentiment" size="small" placeholder="情感" clearable style="width:80px" @change="loadList">
            <el-option label="正面" value="positive"/>
            <el-option label="负面" value="negative"/>
            <el-option label="中性" value="neutral"/>
            <el-option label="混合" value="mixed"/>
          </el-select>
        </div>
        <div class="news-list">
          <div v-for="a in articles" :key="a.article_id"
               :class="['ncard', selId===a.article_id?'sel':'']"
               @click="selectArticle(a)">
            <div class="nc-row1">
              <span class="nc-sector" :class="sectorClass(a.sector_tag)">{{ a.sector_tag }}</span>
              <span class="nc-source">{{ a.source }}</span>
              <span class="nc-time">{{ a.publish_ts?.slice(5,16) }}</span>
            </div>
            <div class="nc-title">{{ a.title }}</div>
            <div class="nc-status">
              <span :class="['ns-badge', a.summarized?'done':'pending']">摘要</span>
              <span :class="['ns-badge', a.sentiment_done?'done':'pending']">情感</span>
              <span :class="['ns-badge', a.extracted?'done':'pending']">标签</span>
              <span v-if="a.ai_sentiment" :class="['sentiment-tag', a.ai_sentiment]">
                {{ sentimentLabel(a.ai_sentiment) }}
                <span v-if="a.sentiment_score!==null" class="score">{{ a.sentiment_score>0?'+':'' }}{{ a.sentiment_score }}</span>
              </span>
              <span class="method-tag" v-if="a.ai_method && a.ai_method!=='PENDING'">{{ a.ai_method==='MOCK'?'模拟':'Doris AI' }}</span>
            </div>
          </div>
        </div>
      </div>

      <!-- 右：详情区 -->
      <div class="detail-area">
        <el-tabs v-model="activeTab" @tab-click="onTab">

          <!-- Tab1: AI 分析对比 -->
          <el-tab-pane label="🤖 AI 分析对比" name="compare">
            <div v-if="selArticle" class="compare-layout">
              <!-- 原文 -->
              <div class="orig-panel">
                <div class="panel-hd">📰 原始资讯</div>
                <div class="orig-title">{{ selArticle.title }}</div>
                <div class="orig-meta">
                  <el-tag size="small" :class="sectorClass(selArticle.sector_tag)">{{ selArticle.sector_tag }}</el-tag>
                  <span class="meta-source">{{ selArticle.source }}</span>
                  <span class="meta-time">{{ selArticle.publish_ts?.slice(0,16) }}</span>
                </div>
                <div class="orig-content">{{ selArticle.content }}</div>
              </div>

              <!-- AI 结果三栏 -->
              <div class="ai-results">

                <!-- AI_SUMMARIZE -->
                <div :class="['ai-card', selArticle.summarized?'done':'empty']">
                  <div class="ai-card-hd">
                    <span class="fn-chip blue">AI_SUMMARIZE</span>
                    <span class="ai-status">{{ selArticle.summarized?'✓ 已完成':'待执行' }}</span>
                    <el-button v-if="!selArticle.summarized" size="small" type="primary" plain
                      :loading="running==='summarize'" @click="runAI('summarize', [selArticle.article_id])">执行</el-button>
                  </div>
                  <div class="ai-card-body" v-if="selArticle.summarized">
                    <div class="ai-result-text">{{ selArticle.ai_summary }}</div>
                  </div>
                  <div class="ai-card-empty" v-else>点击执行后显示摘要结果</div>
                  <div class="ai-sql-snippet">
                    <code>AI_SUMMARIZE(provider, model, api_key, endpoint, content)</code>
                  </div>
                </div>

                <!-- AI_SENTIMENT -->
                <div :class="['ai-card', selArticle.sentiment_done?'done':'empty']">
                  <div class="ai-card-hd">
                    <span class="fn-chip green">AI_SENTIMENT</span>
                    <span class="ai-status">{{ selArticle.sentiment_done?'✓ 已完成':'待执行' }}</span>
                    <el-button v-if="!selArticle.sentiment_done" size="small" type="success" plain
                      :loading="running==='sentiment'" @click="runAI('sentiment', [selArticle.article_id])">执行</el-button>
                  </div>
                  <div class="ai-card-body" v-if="selArticle.sentiment_done">
                    <div class="sentiment-display">
                      <div :class="['sentiment-badge-lg', selArticle.ai_sentiment]">
                        {{ sentimentEmoji(selArticle.ai_sentiment) }} {{ sentimentLabel(selArticle.ai_sentiment) }}
                      </div>
                      <div class="score-bar-wrap">
                        <span class="score-lo">-100</span>
                        <div class="score-bar">
                          <div class="score-fill" :style="scoreStyle(selArticle.sentiment_score)"></div>
                          <div class="score-dot" :style="scoreDotStyle(selArticle.sentiment_score)"></div>
                        </div>
                        <span class="score-hi">+100</span>
                      </div>
                      <div class="score-val">情感分：{{ selArticle.sentiment_score>0?'+':'' }}{{ selArticle.sentiment_score }}</div>
                    </div>
                  </div>
                  <div class="ai-card-empty" v-else>返回值: positive | negative | neutral | mixed</div>
                  <div class="ai-sql-snippet">
                    <code>AI_SENTIMENT(provider, model, api_key, endpoint, content)</code>
                  </div>
                </div>

                <!-- AI_EXTRACT -->
                <div :class="['ai-card', selArticle.extracted?'done':'empty']">
                  <div class="ai-card-hd">
                    <span class="fn-chip orange">AI_EXTRACT</span>
                    <span class="ai-status">{{ selArticle.extracted?'✓ 已完成':'待执行' }}</span>
                    <el-button v-if="!selArticle.extracted" size="small" type="warning" plain
                      :loading="running==='extract'" @click="runAI('extract', [selArticle.article_id])">执行</el-button>
                  </div>
                  <div class="ai-card-body" v-if="selArticle.extracted && parsedExtract">
                    <div class="extract-labels">
                      <div v-for="(val, key) in parsedExtract" :key="key" class="ext-row">
                        <span class="ext-key">{{ key }}</span>
                        <span class="ext-val" v-if="Array.isArray(val)">
                          <el-tag v-for="v in val" :key="v" size="small" type="info" style="margin:1px">{{ v }}</el-tag>
                          <span v-if="!val.length" class="grey">—</span>
                        </span>
                        <span class="ext-val" v-else>{{ val || '—' }}</span>
                      </div>
                    </div>
                  </div>
                  <div class="ai-card-empty" v-else>按给定标签列表从文本中提取结构化信息</div>
                  <div class="ai-sql-snippet">
                    <code>AI_EXTRACT(provider, model, api_key, endpoint, content, ARRAY('事件类型','影响板块',...))</code>
                  </div>
                </div>

              </div>
            </div>
            <div class="empty-tip" v-else>← 点击左侧资讯查看 AI 分析对比</div>
          </el-tab-pane>

          <!-- Tab2: 情感全景 -->
          <el-tab-pane label="📊 情感全景" name="sentiment">
            <div class="two-col">
              <div><div class="ct">情感分布</div><div ref="sentPieChart" class="chart-h260"></div></div>
              <div><div class="ct">各板块情感分布</div><div ref="sentSecChart" class="chart-h260"></div></div>
            </div>
            <div class="two-col" style="margin-top:14px">
              <div><div class="ct">情感分值分布（-100~+100）</div><div ref="scoreChart" class="chart-h220"></div></div>
              <div>
                <div class="ct">情感分析进度</div>
                <div class="sent-table">
                  <div class="sent-row hd"><span>情感</span><span>数量</span><span>占比</span></div>
                  <div class="sent-row" v-for="(cnt, s) in tagData?.sentiment_dist||{}" :key="s">
                    <span><span :class="['sentiment-dot', s]"></span>{{ sentimentLabel(s) }}</span>
                    <span>{{ cnt }}</span>
                    <span>{{ tagData?.total ? (cnt/tagData.total*100).toFixed(1) : 0 }}%</span>
                  </div>
                </div>
                <div class="ct" style="margin-top:14px">SQL 技术说明</div>
                <div class="sql-mini">
                  <pre>SELECT ai_sentiment,
  COUNT(*) AS cnt,
  AVG(sentiment_score) AS avg_score
FROM news_article
WHERE sentiment_done = 1
GROUP BY ai_sentiment
ORDER BY cnt DESC</pre>
                </div>
              </div>
            </div>
          </el-tab-pane>

          <!-- Tab3: 标签分析 -->
          <el-tab-pane label="🏷️ 标签分析" name="tags">
            <div class="two-col">
              <div>
                <div class="ct">AI 提取热词 Top30</div>
                <div class="tag-cloud">
                  <span v-for="t in tagData?.top_tags||[]" :key="t.tag"
                    :class="['tag-word', tagColorClass(t.tag)]"
                    :style="{ fontSize: tagFontSize(t.freq)+'px' }"
                    @click="filterByTag(t)">
                    {{ t.tag.split(':')[1] || t.tag }}
                    <sup>{{ t.freq }}</sup>
                  </span>
                </div>
              </div>
              <div><div class="ct">标签类型分布</div><div ref="tagTypeChart" class="chart-h260"></div></div>
            </div>
            <div class="two-col" style="margin-top:14px">
              <div><div class="ct">板块 × 市场影响方向</div><div ref="impactChart" class="chart-h240"></div></div>
              <div>
                <div class="ct">AI_EXTRACT 标签详情</div>
                <div class="tag-list-panel">
                  <div v-for="t in tagData?.top_tags?.slice(0,15)||[]" :key="t.tag" class="tl-row">
                    <span class="tl-type">{{ t.tag.split(':')[0] }}</span>
                    <span class="tl-val">{{ t.tag.split(':')[1] }}</span>
                    <span class="tl-freq">×{{ t.freq }}</span>
                    <el-progress :percentage="Math.min(t.freq*8, 100)" :stroke-width="4" :show-text="false" style="flex:1;margin-left:8px"/>
                  </div>
                </div>
              </div>
            </div>
            <!-- AI_EXTRACT SQL -->
            <div class="sql-card" style="margin-top:14px">
              <div class="sql-title">⚡ Doris AI_EXTRACT SQL — 结构化标签提取</div>
              <pre class="sql-code">-- AI_EXTRACT 按给定标签列表提取文本中的结构化信息
SELECT article_id, sector_tag,
    AI_EXTRACT(
        '{{ settings.provider }}', 'gpt-4o-mini', '***', 'https://api.openai.com/v1',
        content,
        ARRAY('事件类型', '影响板块', '关键政策或技术', '核心公司', '市场影响方向')
    ) AS ai_extract
FROM news_article
WHERE extracted = 0
LIMIT 25

-- 结果写回 Doris，后续可用 JSON_EXTRACT 进行聚合分析
SELECT JSON_EXTRACT(ai_extract, '$.市场影响方向') AS impact,
       COUNT(*) AS cnt
FROM news_article
WHERE extracted = 1
GROUP BY impact</pre>
            </div>
          </el-tab-pane>

          <!-- Tab4: 指标大盘 -->
          <el-tab-pane label="📈 指标大盘" name="metrics">
            <!-- KPI 行 -->
            <div class="kpi-row" v-if="sectorMetrics.length">
              <div class="kpi-card">
                <div class="kpi-val">{{ sectorMetrics.length }}</div>
                <div class="kpi-label">覆盖板块数</div>
              </div>
              <div class="kpi-card bullish">
                <div class="kpi-val">{{ sectorMetrics.filter(r=>r.avg_score>=35).length }}</div>
                <div class="kpi-label">看多板块</div>
              </div>
              <div class="kpi-card bearish">
                <div class="kpi-val">{{ sectorMetrics.filter(r=>r.avg_score<=-20).length }}</div>
                <div class="kpi-label">看空板块</div>
              </div>
              <div class="kpi-card">
                <div class="kpi-val" :class="overallScore>=0?'pos-val':'neg-val'">
                  {{ overallScore>=0?'+':'' }}{{ overallScore }}
                </div>
                <div class="kpi-label">市场情感均分</div>
              </div>
              <div class="kpi-card">
                <div class="kpi-val blue-val">{{ sectorMetrics.reduce((s,r)=>s+(r.article_count||0),0) }}</div>
                <div class="kpi-label">已分析资讯</div>
              </div>
            </div>

            <div class="two-col" style="margin-top:14px">
              <!-- 板块情感均分横向条形 -->
              <div>
                <div class="ct">板块情感分值对比（AI_SENTIMENT → 量化指标）</div>
                <div ref="sectorBarChart" class="chart-h300"></div>
              </div>
              <!-- 板块指标表格 -->
              <div>
                <div class="ct">板块量化指标明细</div>
                <div class="metrics-table">
                  <div class="mt-hd mt-row">
                    <span>板块</span><span>文章数</span><span>均分</span>
                    <span>正面%</span><span>负面%</span><span>信号</span>
                  </div>
                  <div v-for="r in sectorMetrics" :key="r.sector_tag" class="mt-row">
                    <span><span :class="['nc-sector', sectorClass(r.sector_tag)]">{{ r.sector_tag }}</span></span>
                    <span>{{ r.article_count }}</span>
                    <span :class="r.avg_score>=35?'pos-val':r.avg_score<=-20?'neg-val':''">
                      {{ r.avg_score>=0?'+':'' }}{{ r.avg_score }}
                    </span>
                    <span class="pos-val">{{ r.positive_ratio }}%</span>
                    <span class="neg-val">{{ r.negative_ratio }}%</span>
                    <span>
                      <span :class="['sig-badge', r.avg_score>=35?'bull':r.avg_score<=-20?'bear':'flat']">
                        {{ r.avg_score>=35?'看多':r.avg_score<=-20?'看空':'中性' }}
                      </span>
                    </span>
                  </div>
                </div>
                <div class="sql-mini" style="margin-top:10px">
                  <pre>-- Doris 聚合 SQL（GROUP BY 板块）
SELECT sector_tag,
  AVG(sentiment_score) AS avg_score,
  COUNT(DISTINCT article_id) AS cnt,
  SUM(ai_sentiment='positive') AS pos_cnt
FROM news_article
WHERE sentiment_done = 1
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY article_id
  ORDER BY publish_ts DESC) = 1
GROUP BY sector_tag</pre>
                </div>
              </div>
            </div>

            <!-- 正/负比例面积图 -->
            <div style="margin-top:14px">
              <div class="ct">各板块正负情感分布（堆叠占比）</div>
              <div ref="posNegChart" class="chart-h220"></div>
            </div>

            <div class="empty-tip" v-if="!sectorMetrics.length">请先执行 AI_SENTIMENT 分析</div>
          </el-tab-pane>

          <!-- Tab5: 投资信号 -->
          <el-tab-pane label="⚡ 投资信号" name="signals">
            <div class="two-col">
              <!-- 信号卡片列 -->
              <div>
                <div class="ct">板块投资信号（基于资讯情感综合评分）</div>
                <div class="signal-list">
                  <div v-for="s in signalData" :key="s.sector"
                       :class="['signal-card', s.signal]">
                    <div class="sc-top">
                      <span class="sc-sector">{{ s.sector }}</span>
                      <span :class="['sc-signal', s.signal]">
                        {{ s.signal==='bullish'?'📈 看多':s.signal==='bearish'?'📉 看空':'➖ 中性' }}
                      </span>
                    </div>
                    <div class="sc-mid">
                      <div class="sc-score" :class="s.avg_score>=0?'pos-val':'neg-val'">
                        {{ s.avg_score>=0?'+':'' }}{{ s.avg_score }}
                        <span class="sc-unit">情感分</span>
                      </div>
                      <div class="sc-conf">
                        置信度 {{ s.confidence }}%
                        <el-progress :percentage="s.confidence" :color="s.signal==='bullish'?'#f56c6c':s.signal==='bearish'?'#67c23a':'#909399'"
                          :stroke-width="4" :show-text="false" style="width:80px;display:inline-block;vertical-align:middle;margin-left:6px"/>
                      </div>
                    </div>
                    <div class="sc-detail">
                      <span>共 {{ s.article_count }} 篇</span>
                      <span class="pos-val">正面 {{ s.positive }}</span>
                      <span class="neg-val">负面 {{ s.negative }}</span>
                      <span style="color:#909399">中性 {{ s.neutral }}</span>
                    </div>
                  </div>
                </div>
              </div>

              <!-- 热点公司 -->
              <div>
                <div class="ct">热点公司（AI_EXTRACT 核心公司聚合 Top 20）</div>
                <div class="company-list">
                  <div v-for="c in hotCompanies" :key="c.company" class="co-row">
                    <span class="co-name">{{ c.company }}</span>
                    <div class="co-tags">
                      <el-tag v-for="sec in c.sectors" :key="sec" size="small" :class="sectorClass(sec)" style="margin:1px;font-size:9px">{{ sec }}</el-tag>
                    </div>
                    <span class="co-cnt">×{{ c.count }}</span>
                    <el-progress :percentage="Math.min(c.count*20,100)" :stroke-width="4" :show-text="false"
                      color="#409eff" style="flex:1;margin-left:8px;min-width:60px"/>
                  </div>
                </div>
                <div class="empty-tip" v-if="!hotCompanies.length" style="padding:20px 0">
                  请先执行 AI_EXTRACT 提取标签
                </div>
              </div>
            </div>

            <!-- 信号雷达图 -->
            <div style="margin-top:14px">
              <div class="ct">板块情感雷达（覆盖正/负/均值三维度）</div>
              <div ref="radarChart" class="chart-h300"></div>
            </div>

            <!-- 说明 SQL -->
            <div class="sql-card" style="margin-top:14px">
              <div class="sql-title">⚡ 投资信号生成逻辑 SQL</div>
              <pre class="sql-code">-- Step1: AI_SENTIMENT 写回情感标签
UPDATE news_article
SET ai_sentiment  = AI_SENTIMENT('qwen_llm', content),
    sentiment_done = 1
WHERE sentiment_done = 0

-- Step2: 按板块聚合，生成量化信号
SELECT sector_tag,
  AVG(sentiment_score)         AS avg_score,   -- 情感均分 (-100 ~ +100)
  COUNT(DISTINCT article_id)   AS total,
  SUM(ai_sentiment='positive') / COUNT(*) AS bullish_ratio
FROM news_article WHERE sentiment_done=1
QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY publish_ts DESC)=1
GROUP BY sector_tag
-- avg_score >= 35 → 看多信号；<= -20 → 看空信号</pre>
            </div>
          </el-tab-pane>

          <!-- Tab6: 三函数对比说明 -->
          <el-tab-pane label="📖 函数说明" name="docs">
            <div class="docs-grid">
              <div class="doc-card blue">
                <div class="doc-fn">AI_SUMMARIZE</div>
                <div class="doc-desc">对文本进行高度总结概括，返回简洁摘要</div>
                <div class="doc-sql">
                  <pre>AI_SUMMARIZE(
    provider,    -- 'openai' / 'azure'
    model,       -- 'gpt-4o-mini'
    api_key,
    endpoint,
    text_column  -- 待摘要的文本字段
) → VARCHAR</pre>
                </div>
                <div class="doc-use">📌 适用：资讯摘要、报告概括、长文压缩</div>
                <div class="doc-stat">
                  本批次处理：<b>{{ stats.summarized }}</b> / {{ stats.total }} 条
                </div>
              </div>

              <div class="doc-card green">
                <div class="doc-fn">AI_SENTIMENT</div>
                <div class="doc-desc">分析文本情感倾向，返回枚举值</div>
                <div class="doc-sql">
                  <pre>AI_SENTIMENT(
    provider,
    model,
    api_key,
    endpoint,
    text_column  -- 待分析的文本字段
) → ENUM(
    'positive',  -- 正面/利好
    'negative',  -- 负面/利空
    'neutral',   -- 中性
    'mixed'      -- 混合/分化
)</pre>
                </div>
                <div class="doc-use">📌 适用：资讯情绪监控、舆情预警、投资信号</div>
                <div class="doc-stat">
                  本批次处理：<b>{{ stats.sentiment_done }}</b> / {{ stats.total }} 条
                </div>
              </div>

              <div class="doc-card orange">
                <div class="doc-fn">AI_EXTRACT</div>
                <div class="doc-desc">根据给定标签列表，从文本中提取对应结构化信息</div>
                <div class="doc-sql">
                  <pre>AI_EXTRACT(
    provider,
    model,
    api_key,
    endpoint,
    text_column,  -- 待提取的文本字段
    label_array   -- 提取标签列表
                  -- ARRAY('事件类型','影响板块',
                  --        '核心公司','市场影响方向')
) → JSON          -- {"事件类型":"政策","影响板块":["半导体"],...}</pre>
                </div>
                <div class="doc-use">📌 适用：实体抽取、事件分类、知识图谱构建</div>
                <div class="doc-stat">
                  本批次处理：<b>{{ stats.extracted }}</b> / {{ stats.total }} 条
                </div>
              </div>
            </div>

            <div class="arch-explain">
              <div class="ae-title">⚡ 为什么用 Doris AI Function 而不是外部调用？</div>
              <div class="ae-grid">
                <div class="ae-item">
                  <div class="ae-icon">🏃</div>
                  <div class="ae-label">零数据搬运</div>
                  <div class="ae-desc">文本数据无需从 Doris 导出到应用层，AI 分析在库内完成，延迟降低 60%+</div>
                </div>
                <div class="ae-item">
                  <div class="ae-icon">🔗</div>
                  <div class="ae-label">结果即时可查</div>
                  <div class="ae-desc">AI 输出直接写回同一张表，可立即用 SQL 进行聚合、JOIN 等二次分析</div>
                </div>
                <div class="ae-item">
                  <div class="ae-icon">📊</div>
                  <div class="ae-label">大批量并行</div>
                  <div class="ae-desc">Doris 并行执行框架支持多行同时调用 AI Function，吞吐量远超串行方案</div>
                </div>
                <div class="ae-item">
                  <div class="ae-icon">🔒</div>
                  <div class="ae-label">数据安全</div>
                  <div class="ae-desc">敏感数据不经过应用服务器，降低数据泄露风险，满足合规要求</div>
                </div>
              </div>
            </div>
          </el-tab-pane>

        </el-tabs>
      </div>
    </div>

    <!-- 空态 -->
    <div class="empty-state" v-if="stats.total===0 && !importing">
      <div style="font-size:52px;margin-bottom:12px">📰</div>
      <div style="font-size:15px;font-weight:600;margin-bottom:6px">资讯 AI 分析平台就绪</div>
      <div style="font-size:12px;color:#909399;margin-bottom:16px">点击「导入资讯」写入 25 条模拟金融资讯，再使用三大 AI Function 进行分析</div>
      <el-button type="primary" size="large" :loading="importing" @click="doImport">📥 导入模拟资讯</el-button>
    </div>

  </div>
</template>

<script setup>
import { ref, computed, onMounted, nextTick } from 'vue'
import { ElMessage } from 'element-plus'
import * as echarts from 'echarts'
import { newsApi } from '@/api'

const importing  = ref(false)
const running    = ref('')
const sqlVisible = ref(false)
const lastSql    = ref('')
const activeTab  = ref('compare')

const stats      = ref({ total:0, summarized:0, sentiment_done:0, extracted:0 })
const articles   = ref([])
const selId      = ref('')
const selArticle = ref(null)
const tagData    = ref(null)

const keyword        = ref('')
const filterSector   = ref('')
const filterSentiment= ref('')

const sectors = ['半导体','新能源','消费','医药','金融','军工','宏观','化工','传媒']

const sentPieChart = ref(null)
const sentSecChart = ref(null)
const scoreChart   = ref(null)
const tagTypeChart = ref(null)
const impactChart  = ref(null)
const sectorBarChart = ref(null)
const posNegChart    = ref(null)
const radarChart     = ref(null)
const CHARTS = {}

const sectorMetrics = ref([])
const signalData    = ref([])
const hotCompanies  = ref([])

const overallScore = computed(() => {
  if (!sectorMetrics.value.length) return 0
  const sum = sectorMetrics.value.reduce((s, r) => s + (Number(r.avg_score) || 0), 0)
  return Math.round(sum / sectorMetrics.value.length)
})

function initChart(r, key) {
  if (!r.value) return null
  if (!CHARTS[key]) CHARTS[key] = echarts.init(r.value)
  return CHARTS[key]
}

const canRun = computed(() => stats.value.total > 0)

const parsedExtract = computed(() => {
  if (!selArticle.value?.ai_extract) return null
  try {
    const v = selArticle.value.ai_extract
    return typeof v === 'string' ? JSON.parse(v) : v
  } catch { return null }
})

// ── 工具 ──────────────────────────────────────────────────
const pct = (a, b) => b ? Math.round(a / b * 100) : 0

function sentimentLabel(s) {
  return { positive:'正面', negative:'负面', neutral:'中性', mixed:'混合' }[s] || s
}
function sentimentEmoji(s) {
  return { positive:'📈', negative:'📉', neutral:'➖', mixed:'↕️' }[s] || ''
}
function sectorClass(s) {
  const m = { '半导体':'tag-blue','新能源':'tag-green','消费':'tag-orange','医药':'tag-purple','金融':'tag-red','军工':'tag-dark','宏观':'tag-grey','化工':'tag-cyan','传媒':'tag-pink' }
  return m[s] || 'tag-grey'
}
function tagColorClass(tag) {
  const t = tag.split(':')[0]
  return { '事件类型':'blue','影响板块':'green','关键政策或技术':'orange','核心公司':'purple','市场影响方向':'red' }[t] || ''
}
function tagFontSize(freq) {
  return Math.min(12 + freq * 2, 22)
}
function scoreStyle(score) {
  const pct = ((score || 0) + 100) / 200 * 100
  return { width: Math.abs((score||0)/2) + '%', left: (score||0)>=0 ? '50%' : (50 + (score||0)/2) + '%',
    background: (score||0) >= 0 ? '#f56c6c' : '#67c23a' }
}
function scoreDotStyle(score) {
  const left = ((score || 0) + 100) / 200 * 100
  return { left: left + '%' }
}

// ── 操作 ──────────────────────────────────────────────────
async function doImport() {
  importing.value = true
  try {
    await newsApi.init()
    const r = await newsApi.import()
    ElMessage.success(r.msg)
    await loadAll()
  } catch (e) { ElMessage.error('导入失败') }
  finally { importing.value = false }
}

async function runAI(mode, ids = null) {
  running.value = mode
  try {
    let r
    if (mode === 'summarize') r = await newsApi.summarize(ids)
    else if (mode === 'sentiment') r = await newsApi.sentiment(ids)
    else r = await newsApi.extract(ids)
    ElMessage.success(r.msg)
    if (r.sql) lastSql.value = r.sql
    await loadAll()
    if (selId.value) {
      const found = articles.value.find(a => a.article_id === selId.value)
      if (found) selArticle.value = found
    }
    if (activeTab.value === 'sentiment' || activeTab.value === 'tags') {
      await loadTagData()
    }
  } catch (e) { ElMessage.error('AI 分析失败') }
  finally { running.value = '' }
}

async function loadAll() {
  await Promise.all([loadStats(), loadList()])
}

async function loadStats() {
  stats.value = await newsApi.stats() || {}
}

async function loadList() {
  const params = {}
  if (keyword.value)         params.keyword   = keyword.value
  if (filterSector.value)    params.sector    = filterSector.value
  if (filterSentiment.value) params.sentiment = filterSentiment.value
  articles.value = await newsApi.list(params) || []
}

async function selectArticle(a) {
  selId.value = a.article_id
  const detail = await newsApi.detail(a.article_id)
  selArticle.value = detail
  activeTab.value = 'compare'
}

function filterByTag(t) {
  const val = t.tag.split(':')[1]
  if (val) { keyword.value = val; loadList() }
}

async function onTab({ paneName }) {
  if (paneName === 'sentiment' || paneName === 'tags') {
    await loadTagData()
  }
  if (paneName === 'metrics') {
    await loadMetrics()
  }
  if (paneName === 'signals') {
    await loadSignals()
  }
}

async function loadTagData() {
  tagData.value = await newsApi.tagAnalysis()
  await nextTick()
  if (activeTab.value === 'sentiment') renderSentimentCharts()
  if (activeTab.value === 'tags') renderTagCharts()
}

async function loadMetrics() {
  sectorMetrics.value = await newsApi.sectorMetrics() || []
  await nextTick()
  renderMetricsCharts()
}

async function loadSignals() {
  const [sigs, cos] = await Promise.all([newsApi.signals(), newsApi.hotCompanies()])
  signalData.value   = sigs   || []
  hotCompanies.value = cos    || []
  await nextTick()
  renderRadarChart()
}

// ── 图表 ──────────────────────────────────────────────────
const SENT_COLORS = { positive:'#f56c6c', negative:'#67c23a', neutral:'#909399', mixed:'#e6a23c' }
const SENT_LABELS = { positive:'正面', negative:'负面', neutral:'中性', mixed:'混合' }

function renderSentimentCharts() {
  if (!tagData.value) return
  const dist = tagData.value.sentiment_dist || {}

  // 饼图
  const c1 = initChart(sentPieChart, 'sentpie')
  if (c1) c1.setOption({
    tooltip: { trigger:'item', formatter:'{b}: {c}条 ({d}%)' },
    legend: { bottom:0, textStyle:{fontSize:10} },
    series: [{ type:'pie', radius:['40%','70%'], center:['50%','48%'],
      data: Object.entries(dist).map(([k,v]) => ({ name: SENT_LABELS[k]||k, value:v, itemStyle:{ color: SENT_COLORS[k] } })),
      label: { fontSize:11 },
    }],
  })

  // 板块堆叠
  const secSent = tagData.value.sector_sentiment || {}
  const sectors = Object.keys(secSent)
  const c2 = initChart(sentSecChart, 'sentsec')
  if (c2) c2.setOption({
    tooltip: { trigger:'axis', axisPointer:{ type:'shadow' } },
    legend: { top:0, textStyle:{fontSize:10} },
    grid: { top:28, bottom:60, left:56, right:16 },
    xAxis: { type:'category', data:sectors, axisLabel:{ rotate:30, fontSize:9 } },
    yAxis: { type:'value', axisLabel:{fontSize:9} },
    series: ['positive','negative','neutral','mixed'].map(s => ({
      name: SENT_LABELS[s], type:'bar', stack:'s',
      data: sectors.map(sec => secSent[sec]?.[s] || 0),
      itemStyle: { color: SENT_COLORS[s] },
    })),
  })

  // 情感分值分布直方图
  const allRows = articles.value.filter(a => a.sentiment_score !== null && a.sentiment_score !== undefined)
  const buckets = Array(10).fill(0)
  allRows.forEach(a => {
    const idx = Math.min(Math.floor((Number(a.sentiment_score) + 100) / 20), 9)
    buckets[idx]++
  })
  const c3 = initChart(scoreChart, 'score')
  if (c3) c3.setOption({
    tooltip: { trigger:'axis' },
    grid: { top:18, bottom:32, left:40, right:16 },
    xAxis: { type:'category', data:['-100','-80','-60','-40','-20','0','+20','+40','+60','+80'], axisLabel:{fontSize:9} },
    yAxis: { type:'value', axisLabel:{fontSize:9} },
    series: [{ type:'bar', data: buckets.map((v,i) => ({ value:v, itemStyle:{ color: i<5?'#67c23a':'#f56c6c' } })) }],
  })
}

function renderTagCharts() {
  if (!tagData.value) return
  const topTags = tagData.value.top_tags || []

  // 标签类型饼图
  const typeMap = {}
  topTags.forEach(t => {
    const type = t.tag.split(':')[0]
    typeMap[type] = (typeMap[type] || 0) + t.freq
  })
  const c1 = initChart(tagTypeChart, 'tagtype')
  if (c1) c1.setOption({
    tooltip: { trigger:'item', formatter:'{b}: {c} ({d}%)' },
    legend: { bottom:0, textStyle:{fontSize:10} },
    series: [{ type:'pie', radius:['35%','65%'], center:['50%','46%'],
      data: Object.entries(typeMap).map(([k,v]) => ({ name:k, value:v })),
      label: { fontSize:10 },
    }],
  })

  // 影响方向统计
  const impactMap = {}
  topTags.filter(t => t.tag.startsWith('市场影响方向:')).forEach(t => {
    const v = t.tag.split(':')[1]
    impactMap[v] = (impactMap[v]||0) + t.freq
  })
  const c2 = initChart(impactChart, 'impact')
  if (c2) c2.setOption({
    tooltip: { trigger:'axis' },
    grid: { top:18, bottom:40, left:56, right:16 },
    xAxis: { type:'category', data: Object.keys(impactMap), axisLabel:{fontSize:10} },
    yAxis: { type:'value', axisLabel:{fontSize:9} },
    series: [{ type:'bar',
      data: Object.entries(impactMap).map(([k,v]) => ({
        value:v,
        itemStyle:{ color: k==='利好'?'#f56c6c':k==='利空'?'#67c23a':k==='分化'?'#e6a23c':'#909399' }
      })),
      label:{ show:true, position:'top', fontSize:10 },
    }],
  })
}

function renderMetricsCharts() {
  if (!sectorMetrics.value.length) return
  const data = [...sectorMetrics.value].sort((a, b) => a.avg_score - b.avg_score)
  const sectors = data.map(r => r.sector_tag)
  const scores  = data.map(r => Number(r.avg_score) || 0)

  // 横向条形图
  const c1 = initChart(sectorBarChart, 'sectorbar')
  if (c1) c1.setOption({
    tooltip: { trigger: 'axis', formatter: p => `${p[0].name}<br/>情感均分: ${p[0].value >= 0 ? '+' : ''}${p[0].value}` },
    grid: { top: 10, bottom: 10, left: 72, right: 50, containLabel: false },
    xAxis: { type: 'value', axisLabel: { fontSize: 10 }, splitLine: { lineStyle: { type: 'dashed' } } },
    yAxis: { type: 'category', data: sectors, axisLabel: { fontSize: 10 } },
    series: [{
      type: 'bar', data: scores.map(v => ({
        value: v,
        itemStyle: { color: v >= 35 ? '#f56c6c' : v <= -20 ? '#67c23a' : '#e6a23c' }
      })),
      label: { show: true, position: 'right', fontSize: 10, formatter: p => (p.value >= 0 ? '+' : '') + p.value },
    }],
  })

  // 正负堆叠比例
  const sorted = [...sectorMetrics.value].sort((a, b) => b.positive_ratio - a.positive_ratio)
  const secs2  = sorted.map(r => r.sector_tag)
  const c2 = initChart(posNegChart, 'posneg')
  if (c2) c2.setOption({
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { top: 0, textStyle: { fontSize: 10 } },
    grid: { top: 28, bottom: 40, left: 60, right: 20 },
    xAxis: { type: 'category', data: secs2, axisLabel: { rotate: 30, fontSize: 9 } },
    yAxis: { type: 'value', max: 100, axisLabel: { fontSize: 9, formatter: v => v + '%' } },
    series: [
      { name: '正面%', type: 'bar', stack: 'ratio', data: sorted.map(r => r.positive_ratio), itemStyle: { color: '#f56c6c' } },
      { name: '负面%', type: 'bar', stack: 'ratio', data: sorted.map(r => r.negative_ratio), itemStyle: { color: '#67c23a' } },
      { name: '其余%', type: 'bar', stack: 'ratio', data: sorted.map(r => Math.max(0, 100 - r.positive_ratio - r.negative_ratio)), itemStyle: { color: '#e0e0e0' } },
    ],
  })
}

function renderRadarChart() {
  if (!signalData.value.length) return
  const c = initChart(radarChart, 'radar')
  if (!c) return
  const INDICATORS = [
    { name: '均分(标准化)', max: 100 },
    { name: '正面占比%',   max: 100 },
    { name: '文章覆盖度',  max: Math.max(...signalData.value.map(s => s.article_count)) + 1 },
    { name: '置信度',      max: 100 },
    { name: '看空抵消',    max: 100 },
  ]
  c.setOption({
    tooltip: { trigger: 'item' },
    legend: { bottom: 0, textStyle: { fontSize: 10 }, type: 'scroll' },
    radar: { indicator: INDICATORS, center: ['50%', '50%'], radius: '62%' },
    series: [{
      type: 'radar',
      data: signalData.value.map(s => ({
        name: s.sector,
        value: [
          Math.max(0, (s.avg_score + 100) / 2),
          s.article_count ? Math.round(s.positive / s.article_count * 100) : 0,
          s.article_count,
          s.confidence,
          s.article_count ? Math.round((1 - s.negative / s.article_count) * 100) : 100,
        ],
      })),
    }],
  })
}

onMounted(async () => {
  await loadAll()
})
</script>

<style scoped>
.news-wrap { display:flex; flex-direction:column; gap:14px; }

/* AI 操作栏 */
.ai-bar { display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap; }
.ai-bar-left { flex:1; }
.ai-bar-title { font-size:16px; font-weight:700; color:#1a1a1a; display:flex; align-items:center; gap:8px; margin-bottom:4px; }
.ai-bar-sub   { font-size:12px; color:#909399; }
.doris-badge  { background:#409eff; color:#fff; padding:1px 8px; border-radius:10px; font-size:11px; font-weight:600; }
.ai-actions   { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
.ai-btn-group { display:flex; gap:8px; }
.ai-btn { height:52px; display:flex; flex-direction:column; align-items:center; justify-content:center; padding:0 18px; border-radius:8px !important; border:2px solid; }
.ai-btn .fn-name { font-size:12px; font-weight:700; letter-spacing:.5px; }
.ai-btn .fn-desc { font-size:10px; margin-top:1px; opacity:.75; }
.ai-btn.summarize { border-color:#409eff; color:#409eff; }
.ai-btn.summarize:hover { background:#ecf5ff; }
.ai-btn.sentiment { border-color:#67c23a; color:#67c23a; }
.ai-btn.sentiment:hover { background:#f0f9eb; }
.ai-btn.extract   { border-color:#e6a23c; color:#e6a23c; }
.ai-btn.extract:hover { background:#fdf6ec; }

/* 进度 */
.progress-bar { display:flex; align-items:center; gap:16px; padding:12px 20px; }
.prog-item { display:flex; flex-direction:column; align-items:center; min-width:70px; }
.prog-num  { font-size:24px; font-weight:700; color:#303133; }
.prog-num.blue   { color:#409eff; }
.prog-num.green  { color:#67c23a; }
.prog-num.orange { color:#e6a23c; }
.prog-label { font-size:11px; color:#909399; margin-top:2px; }
.prog-divider { width:1px; height:40px; background:#ebeef5; }

/* SQL 弹窗 */
.sql-dialog-code { background:#1e1e2e; color:#a8b3cf; font-size:11px; padding:14px; border-radius:6px; white-space:pre; overflow-x:auto; line-height:1.7; }
.sql-dialog-tip  { font-size:12px; color:#67c23a; margin-top:8px; background:#f0f9eb; padding:6px 12px; border-radius:4px; }

/* 主区域 */
.main-area { display:grid; grid-template-columns:340px 1fr; gap:14px; }

/* 资讯列表 */
.list-panel { background:#fff; border-radius:8px; box-shadow:0 1px 4px rgba(0,0,0,.06); display:flex; flex-direction:column; height:calc(100vh - 300px); min-height:500px; }
.list-hd    { display:flex; gap:6px; padding:10px; border-bottom:1px solid #f0f0f0; flex-shrink:0; }
.news-list  { flex:1; overflow-y:auto; padding:6px; display:flex; flex-direction:column; gap:5px; }

.ncard { border:1.5px solid #ebeef5; border-radius:8px; padding:10px 12px; cursor:pointer; transition:all .12s; }
.ncard:hover { border-color:#409eff; background:#fafffe; }
.ncard.sel   { border-color:#409eff; background:#ecf5ff; }
.nc-row1 { display:flex; align-items:center; gap:6px; margin-bottom:4px; }
.nc-sector { font-size:10px; font-weight:600; padding:1px 6px; border-radius:8px; }
.nc-source { font-size:10px; color:#c0c4cc; }
.nc-time   { font-size:10px; color:#c0c4cc; margin-left:auto; }
.nc-title  { font-size:12px; font-weight:500; color:#303133; line-height:1.5; margin-bottom:5px; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }
.nc-status { display:flex; align-items:center; gap:4px; flex-wrap:wrap; }
.ns-badge  { font-size:9px; padding:0 5px; border-radius:8px; }
.ns-badge.done    { background:#f0f9eb; color:#67c23a; }
.ns-badge.pending { background:#f5f5f5; color:#c0c4cc; }
.sentiment-tag { font-size:10px; font-weight:600; padding:0 6px; border-radius:8px; margin-left:2px; }
.sentiment-tag.positive { background:#fef0f0; color:#f56c6c; }
.sentiment-tag.negative { background:#f0f9eb; color:#67c23a; }
.sentiment-tag.neutral  { background:#f4f4f5; color:#909399; }
.sentiment-tag.mixed    { background:#fdf6ec; color:#e6a23c; }
.score { font-size:9px; margin-left:2px; }
.method-tag { font-size:9px; color:#c0c4cc; margin-left:auto; }

/* 详情 */
.detail-area { background:#fff; border-radius:8px; padding:16px; box-shadow:0 1px 4px rgba(0,0,0,.06); overflow-y:auto; height:calc(100vh - 300px); min-height:500px; }
.empty-tip { text-align:center; padding:60px 0; font-size:13px; color:#c0c4cc; }

/* 对比布局 */
.compare-layout { display:grid; grid-template-columns:1fr 1fr; gap:14px; }
.orig-panel { border:1px solid #ebeef5; border-radius:8px; padding:14px; overflow-y:auto; max-height:calc(100vh - 380px); }
.panel-hd { font-size:12px; font-weight:600; color:#909399; margin-bottom:8px; }
.orig-title { font-size:14px; font-weight:700; color:#1a1a1a; line-height:1.5; margin-bottom:8px; }
.orig-meta  { display:flex; align-items:center; gap:6px; margin-bottom:10px; }
.meta-source{ font-size:11px; color:#909399; }
.meta-time  { font-size:11px; color:#c0c4cc; }
.orig-content { font-size:12px; color:#606266; line-height:1.8; }

/* AI 结果卡 */
.ai-results { display:flex; flex-direction:column; gap:10px; overflow-y:auto; max-height:calc(100vh - 380px); }
.ai-card { border-radius:8px; padding:12px; border:1.5px solid #ebeef5; transition:all .15s; }
.ai-card.done  { border-color:#67c23a; background:#fafffe; }
.ai-card.empty { background:#fafafa; }
.ai-card-hd { display:flex; align-items:center; gap:6px; margin-bottom:8px; }
.ai-status  { font-size:11px; color:#909399; flex:1; }
.fn-chip    { font-size:11px; font-weight:700; padding:2px 8px; border-radius:10px; letter-spacing:.5px; }
.fn-chip.blue   { background:#ecf5ff; color:#409eff; }
.fn-chip.green  { background:#f0f9eb; color:#67c23a; }
.fn-chip.orange { background:#fdf6ec; color:#e6a23c; }
.ai-card-body  { }
.ai-card-empty { font-size:11px; color:#c0c4cc; font-style:italic; padding:6px 0; }
.ai-result-text { font-size:12px; color:#303133; line-height:1.7; }
.ai-sql-snippet { margin-top:8px; background:#f5f7fa; border-radius:4px; padding:5px 10px; }
.ai-sql-snippet code { font-size:10px; color:#909399; font-family:monospace; }

/* 情感展示 */
.sentiment-display { display:flex; flex-direction:column; align-items:center; gap:8px; padding:4px 0; }
.sentiment-badge-lg { font-size:18px; font-weight:700; padding:6px 18px; border-radius:20px; }
.sentiment-badge-lg.positive { background:#fef0f0; color:#f56c6c; }
.sentiment-badge-lg.negative { background:#f0f9eb; color:#67c23a; }
.sentiment-badge-lg.neutral  { background:#f4f4f5; color:#909399; }
.sentiment-badge-lg.mixed    { background:#fdf6ec; color:#e6a23c; }
.score-bar-wrap { display:flex; align-items:center; gap:6px; width:100%; }
.score-lo { font-size:10px; color:#67c23a; }
.score-hi { font-size:10px; color:#f56c6c; }
.score-bar { flex:1; height:8px; background:#f0f0f0; border-radius:4px; position:relative; }
.score-fill { height:100%; position:absolute; border-radius:4px; }
.score-dot  { width:12px; height:12px; border-radius:50%; background:#303133; position:absolute; top:-2px; transform:translateX(-50%); }
.score-val { font-size:13px; font-weight:700; color:#303133; }

/* AI_EXTRACT 标签 */
.extract-labels { display:flex; flex-direction:column; gap:6px; }
.ext-row  { display:flex; align-items:flex-start; gap:8px; }
.ext-key  { font-size:10px; font-weight:600; color:#909399; white-space:nowrap; min-width:90px; padding-top:2px; }
.ext-val  { font-size:12px; color:#303133; flex:1; }

/* 板块颜色 */
.tag-blue   { background:#ecf5ff; color:#409eff; }
.tag-green  { background:#f0f9eb; color:#67c23a; }
.tag-orange { background:#fdf6ec; color:#e6a23c; }
.tag-purple { background:#f3e8ff; color:#9b59b6; }
.tag-red    { background:#fef0f0; color:#f56c6c; }
.tag-dark   { background:#e9ecef; color:#495057; }
.tag-grey   { background:#f5f5f5; color:#909399; }
.tag-cyan   { background:#e8f8f5; color:#1abc9c; }
.tag-pink   { background:#fce4ec; color:#e91e63; }

/* 情感点 */
.sentiment-dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:4px; }
.sentiment-dot.positive { background:#f56c6c; }
.sentiment-dot.negative { background:#67c23a; }
.sentiment-dot.neutral  { background:#909399; }
.sentiment-dot.mixed    { background:#e6a23c; }

/* 词云 */
.tag-cloud { display:flex; flex-wrap:wrap; gap:8px; padding:12px; min-height:200px; align-content:flex-start; }
.tag-word  { cursor:pointer; border-radius:4px; padding:2px 6px; transition:all .12s; }
.tag-word:hover { transform:scale(1.1); }
.tag-word.blue   { color:#409eff; }
.tag-word.green  { color:#67c23a; }
.tag-word.orange { color:#e6a23c; }
.tag-word.purple { color:#9b59b6; }
.tag-word.red    { color:#f56c6c; }
.tag-word sup    { font-size:9px; color:#c0c4cc; }

/* 标签列表 */
.tag-list-panel { display:flex; flex-direction:column; gap:5px; max-height:240px; overflow-y:auto; }
.tl-row   { display:flex; align-items:center; gap:6px; font-size:11px; }
.tl-type  { color:#c0c4cc; width:60px; font-size:10px; flex-shrink:0; }
.tl-val   { color:#303133; font-weight:500; }
.tl-freq  { color:#909399; font-size:10px; flex-shrink:0; }

/* 情感表格 */
.sent-table { border:1px solid #ebeef5; border-radius:6px; overflow:hidden; }
.sent-row   { display:grid; grid-template-columns:1fr 60px 60px; padding:6px 12px; font-size:12px; border-bottom:1px solid #f5f5f5; }
.sent-row.hd{ background:#f5f7fa; font-weight:600; font-size:11px; color:#606266; }

/* SQL */
.sql-card  { background:#1e1e2e; border-radius:8px; padding:10px 14px; }
.sql-title { font-size:11px; color:#67c23a; font-weight:600; margin-bottom:6px; }
.sql-code  { color:#a8b3cf; font-size:10px; font-family:monospace; white-space:pre; margin:0; line-height:1.7; overflow-x:auto; }
.sql-mini pre { background:#f5f7fa; border-radius:6px; padding:8px 12px; font-size:10px; color:#606266; font-family:monospace; line-height:1.7; margin:0; white-space:pre; overflow-x:auto; }

/* 文档 */
.docs-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:16px; margin-bottom:20px; }
.doc-card  { border-radius:10px; padding:16px; border:2px solid; }
.doc-card.blue   { border-color:#409eff; background:#fafcff; }
.doc-card.green  { border-color:#67c23a; background:#fafff7; }
.doc-card.orange { border-color:#e6a23c; background:#fffdf7; }
.doc-fn    { font-size:15px; font-weight:700; letter-spacing:.5px; margin-bottom:6px; }
.doc-card.blue   .doc-fn { color:#409eff; }
.doc-card.green  .doc-fn { color:#67c23a; }
.doc-card.orange .doc-fn { color:#e6a23c; }
.doc-desc  { font-size:12px; color:#606266; margin-bottom:10px; }
.doc-sql pre { background:#1e1e2e; color:#a8b3cf; font-size:9.5px; border-radius:6px; padding:8px 10px; white-space:pre; overflow-x:auto; line-height:1.7; margin:0 0 10px; }
.doc-use   { font-size:11px; color:#909399; margin-bottom:8px; }
.doc-stat  { font-size:12px; color:#606266; background:#f5f7fa; padding:5px 10px; border-radius:4px; }

/* 架构说明 */
.arch-explain { background:#f9fafc; border-radius:8px; padding:16px; }
.ae-title  { font-size:13px; font-weight:700; color:#1a1a1a; margin-bottom:14px; }
.ae-grid   { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; }
.ae-item   { background:#fff; border-radius:8px; padding:12px; text-align:center; box-shadow:0 1px 4px rgba(0,0,0,.05); }
.ae-icon   { font-size:24px; margin-bottom:6px; }
.ae-label  { font-size:12px; font-weight:600; color:#303133; margin-bottom:4px; }
.ae-desc   { font-size:11px; color:#909399; line-height:1.5; }

/* 指标大盘 */
.kpi-row   { display:grid; grid-template-columns:repeat(5,1fr); gap:12px; }
.kpi-card  { background:#fff; border-radius:8px; padding:14px 16px; box-shadow:0 1px 4px rgba(0,0,0,.06); text-align:center; }
.kpi-card.bullish { border-top:3px solid #f56c6c; }
.kpi-card.bearish { border-top:3px solid #67c23a; }
.kpi-val   { font-size:28px; font-weight:700; color:#303133; }
.kpi-label { font-size:11px; color:#909399; margin-top:4px; }
.pos-val   { color:#f56c6c; }
.neg-val   { color:#67c23a; }
.blue-val  { color:#409eff; }

.metrics-table { border:1px solid #ebeef5; border-radius:6px; overflow:hidden; font-size:12px; }
.mt-hd { background:#f5f7fa; font-weight:600; font-size:11px; color:#606266; }
.mt-row { display:grid; grid-template-columns:80px 56px 56px 60px 60px 56px; padding:6px 10px; border-bottom:1px solid #f5f5f5; align-items:center; gap:4px; }

.sig-badge { font-size:10px; padding:1px 6px; border-radius:8px; font-weight:600; }
.sig-badge.bull { background:#fef0f0; color:#f56c6c; }
.sig-badge.bear { background:#f0f9eb; color:#67c23a; }
.sig-badge.flat { background:#f5f5f5; color:#909399; }

/* 投资信号 */
.signal-list { display:flex; flex-direction:column; gap:8px; max-height:460px; overflow-y:auto; }
.signal-card { border-radius:8px; padding:12px 14px; border:1.5px solid #ebeef5; }
.signal-card.bullish { border-color:#f56c6c; background:#fff9f9; }
.signal-card.bearish { border-color:#67c23a; background:#f9fff9; }
.signal-card.neutral { background:#fafafa; }
.sc-top  { display:flex; align-items:center; justify-content:space-between; margin-bottom:6px; }
.sc-sector { font-size:13px; font-weight:700; color:#303133; }
.sc-signal { font-size:12px; font-weight:600; padding:2px 8px; border-radius:10px; }
.sc-signal.bullish { background:#fef0f0; color:#f56c6c; }
.sc-signal.bearish { background:#f0f9eb; color:#67c23a; }
.sc-signal.neutral { background:#f5f5f5; color:#909399; }
.sc-mid  { display:flex; align-items:center; justify-content:space-between; margin-bottom:6px; }
.sc-score { font-size:20px; font-weight:700; }
.sc-unit  { font-size:10px; color:#909399; margin-left:3px; }
.sc-conf  { font-size:11px; color:#909399; display:flex; align-items:center; }
.sc-detail { display:flex; gap:10px; font-size:11px; color:#909399; }

.company-list { display:flex; flex-direction:column; gap:5px; max-height:400px; overflow-y:auto; }
.co-row  { display:flex; align-items:center; gap:6px; font-size:12px; padding:4px 0; border-bottom:1px solid #f5f5f5; }
.co-name { font-weight:600; color:#303133; width:80px; flex-shrink:0; }
.co-tags { display:flex; flex-wrap:wrap; flex:1; }
.co-cnt  { color:#909399; font-size:11px; flex-shrink:0; }

/* 通用 */
.two-col   { display:grid; grid-template-columns:1fr 1fr; gap:14px; }
.ct        { font-size:13px; font-weight:600; color:#303133; margin-bottom:8px; }
.chart-h220{ height:220px; }
.chart-h240{ height:240px; }
.chart-h260{ height:260px; }
.chart-h300{ height:300px; }
.grey      { color:#c0c4cc; }
.empty-state { text-align:center; padding:80px 0; }
</style>
