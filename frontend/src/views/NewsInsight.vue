<template>
  <div class="news-wrap">

    <!-- 顶栏：导入 + 三大 AI Function 四按钮并排 -->
    <div class="ai-bar card">
      <div class="ai-bar-left">
        <div class="ai-bar-title">
          <span class="doris-badge">⚡ Doris 4.0</span>
          基金资讯 AI Function 分析平台
        </div>
        <div class="ai-bar-sub">AI_SUMMARIZE · AI_SENTIMENT · AI_EXTRACT — 文本语义分析在 Doris 引擎内完成，数据不出库</div>
      </div>
      <div class="ai-actions">
        <el-button class="ai-btn import-btn" :loading="importing" @click="doImport">
          <span class="fn-name">📥 导入资讯</span>
          <span class="fn-desc">生成 1 条随机新闻</span>
        </el-button>
        <el-button class="ai-btn import-btn" @click="showAddDialog">
          <span class="fn-name">✏️ 手动添加</span>
          <span class="fn-desc">自定义资讯内容</span>
        </el-button>
        <el-button class="ai-btn run-all" :loading="running==='all'" :disabled="!canRun" @click="runAllAI">
          <span class="fn-name">🚀 运行 AI 分析</span>
          <span class="fn-desc">SUMMARIZE + SENTIMENT + EXTRACT</span>
        </el-button>
      </div>
    </div>

    <!-- 进度条（有数据时显示） -->
    <div class="progress-bar card" v-if="stats.total > 0">
      <div class="prog-item">
        <div class="prog-num">{{ stats.total }}</div>
        <div class="prog-label">资讯总量</div>
      </div>
      <div class="prog-divider"></div>
      <div class="prog-item">
        <div class="prog-num blue">{{ stats.summarized }}</div>
        <div class="prog-label">已概括</div>
        <el-progress :percentage="pct(stats.summarized, stats.total)" color="#409eff" :stroke-width="4" :show-text="false" style="width:80px;margin-top:4px" />
      </div>
      <div class="prog-divider"></div>
      <div class="prog-item">
        <div class="prog-num green">{{ stats.sentiment_done }}</div>
        <div class="prog-label">已情感分析</div>
        <el-progress :percentage="pct(stats.sentiment_done, stats.total)" color="#67c23a" :stroke-width="4" :show-text="false" style="width:80px;margin-top:4px" />
      </div>
      <div class="prog-divider"></div>
      <div class="prog-item">
        <div class="prog-num orange">{{ stats.extracted }}</div>
        <div class="prog-label">已提取标签</div>
        <el-progress :percentage="pct(stats.extracted, stats.total)" color="#e6a23c" :stroke-width="4" :show-text="false" style="width:80px;margin-top:4px" />
      </div>
      <div class="prog-divider"></div>
      <div class="prog-item" v-if="lastSql">
        <el-button size="small" type="info" plain @click="sqlVisible = true">查看执行 SQL</el-button>
      </div>
    </div>

    <!-- SQL 弹窗 -->
    <el-dialog v-model="sqlVisible" title="Doris AI Function SQL" width="700px">
      <pre class="sql-dialog-code">{{ lastSql }}</pre>
      <div class="sql-dialog-tip">⚡ 该 SQL 在 Doris 引擎内部直接调用 AI Function，数据无需离开数据库</div>
    </el-dialog>

    <!-- 手动添加资讯对话框 -->
    <el-dialog v-model="addDialogVisible" title="手动添加资讯" width="600px" @close="resetAddForm">
      <el-form :model="addForm" label-width="80px">
        <el-form-item label="标题" required>
          <el-input v-model="addForm.title" placeholder="输入资讯标题" maxlength="100" show-word-limit />
        </el-form-item>
        <el-form-item label="内容" required>
          <el-input v-model="addForm.content" type="textarea" placeholder="输入资讯内容" rows="5" maxlength="1000" show-word-limit />
        </el-form-item>
        <el-form-item label="来源" required>
          <el-select v-model="addForm.source" placeholder="选择或输入来源">
            <el-option label="财联社" value="财联社" />
            <el-option label="证券时报" value="证券时报" />
            <el-option label="上海证券报" value="上海证券报" />
            <el-option label="中国证券报" value="中国证券报" />
            <el-option label="其他" value="其他" />
          </el-select>
        </el-form-item>
        <el-form-item label="板块" required>
          <el-select v-model="addForm.sector" placeholder="选择板块">
            <el-option v-for="s in sectors" :key="s" :label="s" :value="s" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="addDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="adding" @click="doAddManual">添加</el-button>
      </template>
    </el-dialog>

    <!-- 主区域：始终渲染，6 个顶层 Tab -->
    <div class="main-area">
      <el-tabs v-model="activeTab" @tab-click="onTab" class="top-tabs">

        <!-- ── Tab1: 资讯列表 ── -->
        <el-tab-pane label="📰 资讯列表" name="articles">
          <div v-if="!stats.total" class="tab-empty">
            <div style="font-size:48px;margin-bottom:12px">📰</div>
            <div style="font-size:16px;font-weight:700;margin-bottom:8px;color:#1a1a1a">资讯 AI 分析平台</div>
            <div style="font-size:13px;color:#606266;margin-bottom:20px;line-height:1.6">
              <div>工作流程：</div>
              <div style="margin-top:8px;padding-left:16px">
                <div>1️⃣ 点击「📥 导入资讯」生成1条随机新闻</div>
                <div>2️⃣ 点击「🚀 运行 AI 分析」执行：</div>
                <div style="margin-left:16px;margin-top:4px">
                  <div>• AI_SUMMARIZE — 文本摘要</div>
                  <div>• AI_SENTIMENT — 情感分析与评分</div>
                  <div>• AI_EXTRACT — 标签提取（事件、公司、板块）</div>
                </div>
                <div style="margin-top:8px">3️⃣ 查看各标签页的分析结果：情感全景、标签分析、板块指标、投资信号等</div>
              </div>
            </div>
            <el-button type="primary" size="large" :loading="importing" @click="doImport" style="font-size:14px;padding:8px 32px">
              📥 {{ importing ? '生成中...' : '生成随机资讯' }}
            </el-button>
          </div>
          <div v-else class="article-layout">
            <!-- 左：列表 -->
            <div class="list-panel">
              <div class="list-hd">
                <el-input v-model="keyword" placeholder="搜索关键词…" size="small" clearable style="flex:1" @change="loadList" />
                <el-select v-model="filterSector" size="small" placeholder="板块" clearable style="width:82px" @change="loadList">
                  <el-option v-for="s in sectors" :key="s" :label="s" :value="s" />
                </el-select>
                <el-select v-model="filterSentiment" size="small" placeholder="情感" clearable style="width:80px" @change="loadList">
                  <el-option label="正面" value="positive" />
                  <el-option label="负面" value="negative" />
                  <el-option label="中性" value="neutral" />
                  <el-option label="混合" value="mixed" />
                </el-select>
              </div>
              <div class="news-list">
                <div
                  v-for="a in articles" :key="a.article_id"
                  :class="['ncard', selId === a.article_id ? 'sel' : '']"
                  @click="selectArticle(a)"
                >
                  <div class="nc-row1">
                    <span class="nc-sector" :class="sectorClass(a.sector_tag)">{{ a.sector_tag }}</span>
                    <span class="nc-source">{{ a.source }}</span>
                    <span class="nc-time">{{ a.publish_ts?.slice(5, 16) }}</span>
                  </div>
                  <div class="nc-title">{{ a.title }}</div>
                  <div class="nc-status">
                    <span :class="['ns-badge', a.summarized ? 'done' : 'pending']">摘要</span>
                    <span :class="['ns-badge', a.sentiment_done ? 'done' : 'pending']">情感</span>
                    <span :class="['ns-badge', a.extracted ? 'done' : 'pending']">标签</span>
                    <span v-if="a.ai_sentiment" :class="['sentiment-tag', a.ai_sentiment]">
                      {{ sentimentLabel(a.ai_sentiment) }}
                      <span v-if="a.sentiment_score !== null" class="score">{{ a.sentiment_score > 0 ? '+' : '' }}{{ a.sentiment_score }}</span>
                    </span>
                    <span class="method-tag" v-if="a.ai_method && a.ai_method !== 'PENDING'">{{ a.ai_method === 'MOCK' ? '模拟' : 'Doris AI' }}</span>
                  </div>
                </div>
                <div v-if="!articles.length" class="list-empty">暂无数据</div>
              </div>
            </div>
            <!-- 右：AI 对比 -->
            <div class="detail-right">
              <div v-if="selArticle">
                <div class="compare-layout">
                  <div class="orig-panel">
                    <div class="panel-hd">📰 原始资讯</div>
                    <div class="orig-title">{{ selArticle.title }}</div>
                    <div class="orig-meta">
                      <el-tag size="small" :class="sectorClass(selArticle.sector_tag)">{{ selArticle.sector_tag }}</el-tag>
                      <span class="meta-source">{{ selArticle.source }}</span>
                      <span class="meta-time">{{ selArticle.publish_ts?.slice(0, 16) }}</span>
                    </div>
                    <div class="orig-content">{{ selArticle.content }}</div>
                  </div>
                  <div class="ai-results">
                    <!-- AI_SUMMARIZE -->
                    <div :class="['ai-card', selArticle.summarized ? 'done' : 'empty']">
                      <div class="ai-card-hd">
                        <span class="fn-chip blue">AI_SUMMARIZE</span>
                        <span class="ai-status">{{ selArticle.summarized ? '✓ 已完成' : '待执行' }}</span>
                        <el-button v-if="!selArticle.summarized" size="small" type="primary" plain :loading="running === 'summarize'" @click="runAI('summarize', [selArticle.article_id])">执行</el-button>
                      </div>
                      <div class="ai-card-body" v-if="selArticle.summarized">
                        <div class="ai-result-text">{{ selArticle.ai_summary }}</div>
                      </div>
                      <div class="ai-card-empty" v-else>点击执行后显示摘要结果</div>
                      <div class="ai-sql-snippet"><code>AI_SUMMARIZE('qwen_llm', content)</code></div>
                    </div>
                    <!-- AI_SENTIMENT -->
                    <div :class="['ai-card', selArticle.sentiment_done ? 'done' : 'empty']">
                      <div class="ai-card-hd">
                        <span class="fn-chip green">AI_SENTIMENT</span>
                        <span class="ai-status">{{ selArticle.sentiment_done ? '✓ 已完成' : '待执行' }}</span>
                        <el-button v-if="!selArticle.sentiment_done" size="small" type="success" plain :loading="running === 'sentiment'" @click="runAI('sentiment', [selArticle.article_id])">执行</el-button>
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
                          <div class="score-val">情感分：{{ selArticle.sentiment_score > 0 ? '+' : '' }}{{ selArticle.sentiment_score }}</div>
                        </div>
                      </div>
                      <div class="ai-card-empty" v-else>返回值: positive | negative | neutral | mixed</div>
                      <div class="ai-sql-snippet"><code>AI_SENTIMENT('qwen_llm', content)</code></div>
                    </div>
                    <!-- AI_EXTRACT -->
                    <div :class="['ai-card', selArticle.extracted ? 'done' : 'empty']">
                      <div class="ai-card-hd">
                        <span class="fn-chip orange">AI_EXTRACT</span>
                        <span class="ai-status">{{ selArticle.extracted ? '✓ 已完成' : '待执行' }}</span>
                        <el-button v-if="!selArticle.extracted" size="small" type="warning" plain :loading="running === 'extract'" @click="runAI('extract', [selArticle.article_id])">执行</el-button>
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
                      <div class="ai-sql-snippet"><code>AI_EXTRACT('qwen_llm', content, ARRAY(...))</code></div>
                    </div>
                  </div>
                </div>
              </div>
              <div class="empty-tip" v-else>← 点击左侧资讯查看 AI 分析对比</div>
            </div>
          </div>
        </el-tab-pane>

        <!-- ── Tab2: 情感全景 ── -->
        <el-tab-pane label="📊 情感全景" name="sentiment">
          <div class="two-col">
            <div>
              <div class="ct">情感分布</div>
              <div ref="sentPieChart" class="chart-h260"></div>
            </div>
            <div>
              <div class="ct">各板块情感分布</div>
              <div ref="sentSecChart" class="chart-h260"></div>
            </div>
          </div>
          <div class="two-col" style="margin-top:14px">
            <div>
              <div class="ct">情感分值分布（-100~+100）</div>
              <div ref="scoreChart" class="chart-h220"></div>
            </div>
            <div>
              <div class="ct">情感分析进度</div>
              <div class="sent-table">
                <div class="sent-row hd"><span>情感</span><span>数量</span><span>占比</span></div>
                <div class="sent-row" v-for="(cnt, s) in tagData?.sentiment_dist || {}" :key="s">
                  <span><span :class="['sentiment-dot', s]"></span>{{ sentimentLabel(s) }}</span>
                  <span>{{ cnt }}</span>
                  <span>{{ tagData?.total ? (cnt / tagData.total * 100).toFixed(1) : 0 }}%</span>
                </div>
              </div>
              <div class="ct" style="margin-top:14px">SQL 技术说明</div>
              <div class="sql-mini">
                <pre>SELECT ai_sentiment, COUNT(*) AS cnt,
  AVG(sentiment_score) AS avg_score
FROM news_article
WHERE sentiment_done = 1
GROUP BY ai_sentiment</pre>
              </div>
            </div>
          </div>
          <div class="empty-tip" v-if="!tagData">请先执行 AI_SENTIMENT 分析</div>
        </el-tab-pane>

        <!-- ── Tab3: 标签分析 ── -->
        <el-tab-pane label="🏷️ 标签分析" name="tags">
          <div class="two-col">
            <div>
              <div class="ct">AI 提取热词 Top30</div>
              <div class="tag-cloud">
                <span
                  v-for="t in tagData?.top_tags || []" :key="t.tag"
                  :class="['tag-word', tagColorClass(t.tag)]"
                  :style="{ fontSize: tagFontSize(t.freq) + 'px' }"
                  @click="filterByTag(t)"
                >
                  {{ t.tag.split(':')[1] || t.tag }}<sup>{{ t.freq }}</sup>
                </span>
              </div>
            </div>
            <div>
              <div class="ct">标签类型分布</div>
              <div ref="tagTypeChart" class="chart-h260"></div>
            </div>
          </div>
          <div class="two-col" style="margin-top:14px">
            <div>
              <div class="ct">板块 × 市场影响方向</div>
              <div ref="impactChart" class="chart-h240"></div>
            </div>
            <div>
              <div class="ct">AI_EXTRACT 标签详情</div>
              <div class="tag-list-panel">
                <div v-for="t in tagData?.top_tags?.slice(0, 15) || []" :key="t.tag" class="tl-row">
                  <span class="tl-type">{{ t.tag.split(':')[0] }}</span>
                  <span class="tl-val">{{ t.tag.split(':')[1] }}</span>
                  <span class="tl-freq">×{{ t.freq }}</span>
                  <el-progress :percentage="Math.min(t.freq * 8, 100)" :stroke-width="4" :show-text="false" style="flex:1;margin-left:8px" />
                </div>
              </div>
            </div>
          </div>
          <div class="empty-tip" v-if="!tagData">请先执行 AI_EXTRACT 提取标签</div>
        </el-tab-pane>

        <!-- ── Tab4: 指标大盘 ── -->
        <el-tab-pane label="📈 指标大盘" name="metrics">
          <div class="kpi-row" v-if="sectorMetrics.length">
            <div class="kpi-card">
              <div class="kpi-val">{{ sectorMetrics.length }}</div>
              <div class="kpi-label">覆盖板块数</div>
            </div>
            <div class="kpi-card bullish">
              <div class="kpi-val">{{ sectorMetrics.filter(r => r.avg_score >= 35).length }}</div>
              <div class="kpi-label">看多板块</div>
            </div>
            <div class="kpi-card bearish">
              <div class="kpi-val">{{ sectorMetrics.filter(r => r.avg_score <= -20).length }}</div>
              <div class="kpi-label">看空板块</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-val" :class="overallScore >= 0 ? 'pos-val' : 'neg-val'">
                {{ overallScore >= 0 ? '+' : '' }}{{ overallScore }}
              </div>
              <div class="kpi-label">市场情感均分</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-val blue-val">{{ sectorMetrics.reduce((s, r) => s + (r.article_count || 0), 0) }}</div>
              <div class="kpi-label">已分析资讯</div>
            </div>
          </div>
          <div class="two-col" style="margin-top:14px">
            <div>
              <div class="ct">板块情感分值对比</div>
              <div ref="sectorBarChart" class="chart-h300"></div>
            </div>
            <div>
              <div class="ct">板块量化指标明细</div>
              <div class="metrics-table">
                <div class="mt-hd mt-row">
                  <span>板块</span><span>文章数</span><span>均分</span><span>正面%</span><span>负面%</span><span>信号</span>
                </div>
                <div v-for="r in sectorMetrics" :key="r.sector_tag" class="mt-row">
                  <span><span :class="['nc-sector', sectorClass(r.sector_tag)]">{{ r.sector_tag }}</span></span>
                  <span>{{ r.article_count }}</span>
                  <span :class="r.avg_score >= 35 ? 'pos-val' : r.avg_score <= -20 ? 'neg-val' : ''">
                    {{ r.avg_score >= 0 ? '+' : '' }}{{ r.avg_score }}
                  </span>
                  <span class="pos-val">{{ r.positive_ratio }}%</span>
                  <span class="neg-val">{{ r.negative_ratio }}%</span>
                  <span>
                    <span :class="['sig-badge', r.avg_score >= 35 ? 'bull' : r.avg_score <= -20 ? 'bear' : 'flat']">
                      {{ r.avg_score >= 35 ? '看多' : r.avg_score <= -20 ? '看空' : '中性' }}
                    </span>
                  </span>
                </div>
              </div>
            </div>
          </div>
          <div style="margin-top:14px">
            <div class="ct">各板块正负情感分布（堆叠占比）</div>
            <div ref="posNegChart" class="chart-h220"></div>
          </div>
          <div class="empty-tip" v-if="!sectorMetrics.length">请先执行 AI_SENTIMENT 分析</div>
        </el-tab-pane>

        <!-- ── Tab5: 投资信号 ── -->
        <el-tab-pane label="⚡ 投资信号" name="signals">
          <div class="two-col">
            <div>
              <div class="ct">板块投资信号</div>
              <div class="signal-list">
                <div v-for="s in signalData" :key="s.sector" :class="['signal-card', s.signal]">
                  <div class="sc-top">
                    <span class="sc-sector">{{ s.sector }}</span>
                    <span :class="['sc-signal', s.signal]">
                      {{ s.signal === 'bullish' ? '📈 看多' : s.signal === 'bearish' ? '📉 看空' : '➖ 中性' }}
                    </span>
                  </div>
                  <div class="sc-mid">
                    <div class="sc-score" :class="s.avg_score >= 0 ? 'pos-val' : 'neg-val'">
                      {{ s.avg_score >= 0 ? '+' : '' }}{{ s.avg_score }}
                      <span class="sc-unit">情感分</span>
                    </div>
                    <div class="sc-conf">
                      置信度 {{ s.confidence }}%
                      <el-progress
                        :percentage="s.confidence"
                        :color="s.signal === 'bullish' ? '#f56c6c' : s.signal === 'bearish' ? '#67c23a' : '#909399'"
                        :stroke-width="4" :show-text="false"
                        style="width:80px;display:inline-block;vertical-align:middle;margin-left:6px"
                      />
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
              <div class="empty-tip" v-if="!signalData.length">请先执行 AI_SENTIMENT 分析</div>
            </div>
            <div>
              <div class="ct">热点公司（AI_EXTRACT 核心公司 Top20）</div>
              <div class="company-list">
                <div v-for="c in hotCompanies" :key="c.company" class="co-row">
                  <span class="co-name">{{ c.company }}</span>
                  <div class="co-tags">
                    <el-tag v-for="sec in c.sectors" :key="sec" size="small" :class="sectorClass(sec)" style="margin:1px;font-size:9px">{{ sec }}</el-tag>
                  </div>
                  <span class="co-cnt">×{{ c.count }}</span>
                  <el-progress :percentage="Math.min(c.count * 20, 100)" :stroke-width="4" :show-text="false" color="#409eff" style="flex:1;margin-left:8px;min-width:60px" />
                </div>
              </div>
              <div class="empty-tip" v-if="!hotCompanies.length">请先执行 AI_EXTRACT 提取标签</div>
              <div style="margin-top:14px">
                <div class="ct">板块情感雷达</div>
                <div ref="radarChart" class="chart-h300"></div>
              </div>
            </div>
          </div>
        </el-tab-pane>

        <!-- ── Tab6: 函数说明 ── -->
        <el-tab-pane label="📖 函数说明" name="docs">
          <div class="docs-grid">
            <div class="doc-card blue">
              <div class="doc-fn">AI_SUMMARIZE</div>
              <div class="doc-desc">对文本进行高度总结概括，返回简洁摘要</div>
              <div class="doc-sql">
                <pre>AI_SUMMARIZE('qwen_llm', text_column) → VARCHAR</pre>
              </div>
              <div class="doc-use">📌 适用：资讯摘要、报告概括、长文压缩</div>
              <div class="doc-stat">本批次处理：<b>{{ stats.summarized }}</b> / {{ stats.total }} 条</div>
            </div>
            <div class="doc-card green">
              <div class="doc-fn">AI_SENTIMENT</div>
              <div class="doc-desc">分析文本情感倾向，返回枚举值</div>
              <div class="doc-sql">
                <pre>AI_SENTIMENT('qwen_llm', text_column)
→ 'positive' | 'negative' | 'neutral' | 'mixed'</pre>
              </div>
              <div class="doc-use">📌 适用：资讯情绪监控、舆情预警、投资信号</div>
              <div class="doc-stat">本批次处理：<b>{{ stats.sentiment_done }}</b> / {{ stats.total }} 条</div>
            </div>
            <div class="doc-card orange">
              <div class="doc-fn">AI_EXTRACT</div>
              <div class="doc-desc">根据给定标签列表，从文本中提取对应结构化信息</div>
              <div class="doc-sql">
                <pre>AI_EXTRACT('qwen_llm', text_column,
  ARRAY('事件类型','影响板块','核心公司','市场影响方向')
) → JSON</pre>
              </div>
              <div class="doc-use">📌 适用：实体抽取、事件分类、知识图谱构建</div>
              <div class="doc-stat">本批次处理：<b>{{ stats.extracted }}</b> / {{ stats.total }} 条</div>
            </div>
          </div>
          <div class="arch-explain">
            <div class="ae-title">⚡ 为什么用 Doris AI Function 而不是外部调用？</div>
            <div class="ae-grid">
              <div class="ae-item">
                <div class="ae-icon">🏃</div>
                <div class="ae-label">零数据搬运</div>
                <div class="ae-desc">文本数据无需从 Doris 导出，AI 分析在库内完成，延迟降低 60%+</div>
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
</template>

<script setup>
import { ref, computed, onMounted, nextTick } from 'vue'
import { ElMessage } from 'element-plus'
import * as echarts from 'echarts'
import { newsApi } from '@/api'

const importing   = ref(false)
const running     = ref('')
const adding      = ref(false)
const sqlVisible  = ref(false)
const lastSql     = ref('')
const activeTab   = ref('articles')
const addDialogVisible = ref(false)
const addForm     = ref({ title: '', content: '', source: '财联社', sector: '半导体' })

const stats      = ref({ total: 0, summarized: 0, sentiment_done: 0, extracted: 0 })
const articles   = ref([])
const selId      = ref('')
const selArticle = ref(null)
const tagData    = ref(null)

const keyword         = ref('')
const filterSector    = ref('')
const filterSentiment = ref('')

const sectors = ['半导体', '新能源', '消费', '医药', '金融', '军工', '宏观', '化工', '传媒']

const sectorMetrics = ref([])
const signalData    = ref([])
const hotCompanies  = ref([])

// chart refs
const sentPieChart   = ref(null)
const sentSecChart   = ref(null)
const scoreChart     = ref(null)
const tagTypeChart   = ref(null)
const impactChart    = ref(null)
const sectorBarChart = ref(null)
const posNegChart    = ref(null)
const radarChart     = ref(null)
const CHARTS = {}

// computed
const canRun = computed(() => stats.value.total > 0)

const overallScore = computed(() => {
  if (!sectorMetrics.value.length) return 0
  const sum = sectorMetrics.value.reduce((s, r) => s + (Number(r.avg_score) || 0), 0)
  return Math.round(sum / sectorMetrics.value.length)
})

function parseExtract(ex) {
  if (!ex) return null
  if (typeof ex !== 'string') return ex
  const s = ex.trim()
  // 尝试 JSON
  if (s.startsWith('{')) {
    try { return JSON.parse(s) } catch {}
  }
  // 解析 key=value 格式（Doris AI_EXTRACT 实际返回格式）
  const result = {}
  for (const pair of s.split(/,(?=[\u4e00-\u9fa5])/)) {
    const i = pair.indexOf('=')
    if (i === -1) continue
    const key = pair.slice(0, i).trim()
    const val = pair.slice(i + 1).trim()
    if (!val) continue
    result[key] = val.includes('、') ? val.split('、').map(v => v.trim()).filter(Boolean) : val
  }
  return Object.keys(result).length ? result : null
}

const parsedExtract = computed(() => parseExtract(selArticle.value?.ai_extract))

// helpers
const pct = (a, b) => b ? Math.round(a / b * 100) : 0

function sentimentLabel(s) {
  return { positive: '正面', negative: '负面', neutral: '中性', mixed: '混合' }[s] || s
}
function sentimentEmoji(s) {
  return { positive: '📈', negative: '📉', neutral: '➖', mixed: '↕️' }[s] || ''
}
function sectorClass(s) {
  const m = { '半导体': 'tag-blue', '新能源': 'tag-green', '消费': 'tag-orange', '医药': 'tag-purple', '金融': 'tag-red', '军工': 'tag-dark', '宏观': 'tag-grey', '化工': 'tag-cyan', '传媒': 'tag-pink' }
  return m[s] || 'tag-grey'
}
function tagColorClass(tag) {
  const t = tag.split(':')[0]
  return { '事件类型': 'blue', '影响板块': 'green', '关键政策或技术': 'orange', '核心公司': 'purple', '市场影响方向': 'red' }[t] || ''
}
function tagFontSize(freq) { return Math.min(12 + freq * 2, 22) }
function scoreStyle(score) {
  return {
    width: Math.abs((score || 0) / 2) + '%',
    left: (score || 0) >= 0 ? '50%' : (50 + (score || 0) / 2) + '%',
    background: (score || 0) >= 0 ? '#f56c6c' : '#67c23a'
  }
}
function scoreDotStyle(score) {
  return { left: ((score || 0) + 100) / 200 * 100 + '%' }
}

function initChart(r, key) {
  if (!r.value) return null
  if (!CHARTS[key]) CHARTS[key] = echarts.init(r.value)
  return CHARTS[key]
}

// actions
async function doImport() {
  importing.value = true
  try {
    await newsApi.init()
    const r = await newsApi.import()
    ElMessage.success(r.msg)
    await loadAll()
    loadTagData()
    loadMetrics()
    loadSignals()
  } catch (e) {
    console.error(e)
    ElMessage.error('导入失败：' + (e.message || '未知错误'))
  }
  finally { importing.value = false }
}

async function runAllAI() {
  running.value = 'all'
  try {
    const r = await newsApi.runAllAI()
    ElMessage.success(r.msg)
    if (r.sql) lastSql.value = r.sql
    await loadAll()
    // 重新加载所有图表数据
    await Promise.all([loadTagData(), loadMetrics(), loadSignals()])
    // 更新当前选中的文章
    if (selId.value) {
      const found = articles.value.find(a => a.article_id === selId.value)
      if (found) selArticle.value = found
    }
  } catch (e) {
    console.error(e)
    ElMessage.error('AI 分析失败：' + (e.message || '未知错误'))
  }
  finally { running.value = '' }
}

function showAddDialog() {
  addDialogVisible.value = true
}

function resetAddForm() {
  addForm.value = { title: '', content: '', source: '财联社', sector: '半导体' }
}

async function doAddManual() {
  if (!addForm.value.title || !addForm.value.content || !addForm.value.source || !addForm.value.sector) {
    ElMessage.warning('请填写所有字段')
    return
  }
  adding.value = true
  try {
    const r = await newsApi.addManual(addForm.value.title, addForm.value.content, addForm.value.source, addForm.value.sector)
    ElMessage.success(r.msg)
    addDialogVisible.value = false
    resetAddForm()
    await loadAll()
    loadTagData()
    loadMetrics()
    loadSignals()
  } catch (e) {
    console.error(e)
    ElMessage.error('添加失败：' + (e.message || '未知错误'))
  }
  finally { adding.value = false }
}

async function loadAll() {
  await Promise.all([loadStats(), loadList()])
}

async function loadStats() {
  try {
    const r = await newsApi.stats()
    stats.value = r || { total: 0, summarized: 0, sentiment_done: 0, extracted: 0 }
  } catch { /* keep current */ }
}

async function loadList() {
  try {
    const params = {}
    if (keyword.value)         params.keyword   = keyword.value
    if (filterSector.value)    params.sector    = filterSector.value
    if (filterSentiment.value) params.sentiment = filterSentiment.value
    const res = await newsApi.list(params)
    articles.value = Array.isArray(res) ? res : (res?.data || [])
  } catch { articles.value = [] }
}

async function selectArticle(a) {
  selId.value = a.article_id
  try {
    const res = await newsApi.detail(a.article_id)
    selArticle.value = res?.data || res || a
  } catch { selArticle.value = a }
  activeTab.value = 'articles'
}

function filterByTag(t) {
  const val = t.tag.split(':')[1]
  if (val) { keyword.value = val; loadList() }
}

async function onTab() {
  const name = activeTab.value
  if (name === 'sentiment' || name === 'tags') await loadTagData()
  if (name === 'metrics') await loadMetrics()
  if (name === 'signals') await loadSignals()
}

async function loadTagData() {
  try { tagData.value = await newsApi.tagAnalysis() } catch { tagData.value = null }
  await nextTick()
  if (activeTab.value === 'sentiment') renderSentimentCharts()
  if (activeTab.value === 'tags') renderTagCharts()
}

async function loadMetrics() {
  try { sectorMetrics.value = await newsApi.sectorMetrics() || [] } catch { sectorMetrics.value = [] }
  await nextTick()
  renderMetricsCharts()
}

async function loadSignals() {
  try {
    const [sigs, cos] = await Promise.all([newsApi.signals(), newsApi.hotCompanies()])
    signalData.value   = sigs || []
    hotCompanies.value = cos  || []
  } catch { signalData.value = []; hotCompanies.value = [] }
  await nextTick()
  renderRadarChart()
}

// chart renderers
const SENT_COLORS = { positive: '#f56c6c', negative: '#67c23a', neutral: '#909399', mixed: '#e6a23c' }
const SENT_LABELS = { positive: '正面', negative: '负面', neutral: '中性', mixed: '混合' }

function renderSentimentCharts() {
  if (!tagData.value) return
  const dist = tagData.value.sentiment_dist || {}

  // 第一个图表：立即渲染
  const c1 = initChart(sentPieChart, 'sentpie')
  if (c1) c1.setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {c}条 ({d}%)' },
    legend: { bottom: 0, textStyle: { fontSize: 10 } },
    series: [{ type: 'pie', radius: ['40%', '70%'], center: ['50%', '48%'],
      data: Object.entries(dist).map(([k, v]) => ({ name: SENT_LABELS[k] || k, value: v, itemStyle: { color: SENT_COLORS[k] } })),
      label: { fontSize: 11 },
    }],
  })

  // 第二、三个图表：延迟渲染（避免UI阻塞）
  requestAnimationFrame(() => {
    const secSent = tagData.value.sector_sentiment || {}
    const secs = Object.keys(secSent)
    const c2 = initChart(sentSecChart, 'sentsec')
    if (c2) c2.setOption({
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      legend: { top: 0, textStyle: { fontSize: 10 } },
      grid: { top: 28, bottom: 60, left: 56, right: 16 },
      xAxis: { type: 'category', data: secs, axisLabel: { rotate: 30, fontSize: 9 } },
      yAxis: { type: 'value', axisLabel: { fontSize: 9 } },
      series: ['positive', 'negative', 'neutral', 'mixed'].map(s => ({
        name: SENT_LABELS[s], type: 'bar', stack: 's',
        data: secs.map(sec => secSent[sec]?.[s] || 0),
        itemStyle: { color: SENT_COLORS[s] },
      })),
    })

    // 分值分布：直接用后端计算的 score_buckets（不再遍历）
    const buckets = tagData.value.score_buckets || Array(10).fill(0)
    const c3 = initChart(scoreChart, 'score')
    if (c3) c3.setOption({
      tooltip: { trigger: 'axis' },
      grid: { top: 18, bottom: 32, left: 40, right: 16 },
      xAxis: { type: 'category', data: ['-100', '-80', '-60', '-40', '-20', '0', '+20', '+40', '+60', '+80'], axisLabel: { fontSize: 9 } },
      yAxis: { type: 'value', axisLabel: { fontSize: 9 } },
      series: [{ type: 'bar', data: buckets.map((v, i) => ({ value: v, itemStyle: { color: i < 5 ? '#67c23a' : '#f56c6c' } })) }],
    })
  })
}

function renderTagCharts() {
  if (!tagData.value) return
  const topTags = tagData.value.top_tags || []

  // 第一个图表：立即渲染
  const typeMap = {}
  topTags.forEach(t => { const type = t.tag.split(':')[0]; typeMap[type] = (typeMap[type] || 0) + t.freq })
  const c1 = initChart(tagTypeChart, 'tagtype')
  if (c1) c1.setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    legend: { bottom: 0, textStyle: { fontSize: 10 } },
    series: [{ type: 'pie', radius: ['35%', '65%'], center: ['50%', '46%'],
      data: Object.entries(typeMap).map(([k, v]) => ({ name: k, value: v })),
      label: { fontSize: 10 },
    }],
  })

  // 第二个图表：延迟渲染
  requestAnimationFrame(() => {
    const impactMap = {}
    topTags.filter(t => t.tag.startsWith('市场影响方向:')).forEach(t => {
      const v = t.tag.split(':')[1]
      impactMap[v] = (impactMap[v] || 0) + t.freq
    })
    const c2 = initChart(impactChart, 'impact')
    if (c2) c2.setOption({
      tooltip: { trigger: 'axis' },
      grid: { top: 18, bottom: 40, left: 56, right: 16 },
      xAxis: { type: 'category', data: Object.keys(impactMap), axisLabel: { fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { fontSize: 9 } },
      series: [{ type: 'bar',
        data: Object.entries(impactMap).map(([k, v]) => ({ value: v, itemStyle: { color: k === '利好' ? '#f56c6c' : k === '利空' ? '#67c23a' : k === '分化' ? '#e6a23c' : '#909399' } })),
        label: { show: true, position: 'top', fontSize: 10 },
      }],
    })
  })
}

function renderMetricsCharts() {
  if (!sectorMetrics.value.length) return
  const data = [...sectorMetrics.value].sort((a, b) => a.avg_score - b.avg_score)
  const c1 = initChart(sectorBarChart, 'sectorbar')
  if (c1) c1.setOption({
    tooltip: { trigger: 'axis', formatter: p => `${p[0].name}<br/>均分: ${p[0].value >= 0 ? '+' : ''}${p[0].value}` },
    grid: { top: 10, bottom: 10, left: 72, right: 50, containLabel: false },
    xAxis: { type: 'value', axisLabel: { fontSize: 10 } },
    yAxis: { type: 'category', data: data.map(r => r.sector_tag), axisLabel: { fontSize: 10 } },
    series: [{ type: 'bar', data: data.map(r => ({ value: Number(r.avg_score) || 0, itemStyle: { color: r.avg_score >= 35 ? '#f56c6c' : r.avg_score <= -20 ? '#67c23a' : '#e6a23c' } })),
      label: { show: true, position: 'right', fontSize: 10, formatter: p => (p.value >= 0 ? '+' : '') + p.value },
    }],
  })
  const sorted = [...sectorMetrics.value].sort((a, b) => b.positive_ratio - a.positive_ratio)
  const c2 = initChart(posNegChart, 'posneg')
  if (c2) c2.setOption({
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { top: 0, textStyle: { fontSize: 10 } },
    grid: { top: 28, bottom: 40, left: 60, right: 20 },
    xAxis: { type: 'category', data: sorted.map(r => r.sector_tag), axisLabel: { rotate: 30, fontSize: 9 } },
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
  c.setOption({
    tooltip: { trigger: 'item' },
    legend: { bottom: 0, textStyle: { fontSize: 10 }, type: 'scroll' },
    radar: {
      indicator: [
        { name: '均分(标准化)', max: 100 },
        { name: '正面占比%', max: 100 },
        { name: '文章覆盖度', max: Math.max(...signalData.value.map(s => s.article_count)) + 1 },
        { name: '置信度', max: 100 },
        { name: '看空抵消', max: 100 },
      ],
      center: ['50%', '50%'], radius: '62%',
    },
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
  // 后台预加载图表数据，切换 tab 时直接渲染
  loadTagData()
  loadMetrics()
  loadSignals()
})
</script>

<style scoped>
.news-wrap { display:flex; flex-direction:column; gap:14px; }

/* 顶栏 */
.ai-bar { display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap; }
.ai-bar-left { flex:1; }
.ai-bar-title { font-size:16px; font-weight:700; color:#1a1a1a; display:flex; align-items:center; gap:8px; margin-bottom:4px; }
.ai-bar-sub   { font-size:12px; color:#909399; }
.doris-badge  { background:#409eff; color:#fff; padding:1px 8px; border-radius:10px; font-size:11px; font-weight:600; }
.ai-actions   { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
.ai-btn { height:52px; display:flex; flex-direction:column; align-items:center; justify-content:center; padding:0 16px; border-radius:8px !important; border:2px solid; }
.ai-btn .fn-name { font-size:12px; font-weight:700; letter-spacing:.5px; }
.ai-btn .fn-desc { font-size:10px; margin-top:1px; opacity:.75; }
.ai-btn.import-btn { border-color:#606266; color:#606266; }
.ai-btn.import-btn:hover { background:#f4f4f5; }
.ai-btn.run-all { border-color:#ff6b00; color:#ff6b00; }
.ai-btn.run-all:hover { background:#fff0e6; }

/* 进度 */
.progress-bar { display:flex; align-items:center; gap:16px; padding:12px 20px; }
.prog-item { display:flex; flex-direction:column; align-items:center; min-width:70px; }
.prog-num  { font-size:24px; font-weight:700; color:#303133; }
.prog-num.blue   { color:#409eff; }
.prog-num.green  { color:#67c23a; }
.prog-num.orange { color:#e6a23c; }
.prog-label { font-size:11px; color:#909399; margin-top:2px; }
.prog-divider { width:1px; height:40px; background:#ebeef5; }

/* SQL */
.sql-dialog-code { background:#1e1e2e; color:#a8b3cf; font-size:11px; padding:14px; border-radius:6px; white-space:pre; overflow-x:auto; line-height:1.7; }
.sql-dialog-tip  { font-size:12px; color:#67c23a; margin-top:8px; background:#f0f9eb; padding:6px 12px; border-radius:4px; }

/* 主区域 */
.main-area { background:#fff; border-radius:8px; box-shadow:0 1px 4px rgba(0,0,0,.06); padding:0 16px 16px; }
.top-tabs { width:100%; }
.top-tabs :deep(.el-tabs__header) { margin-bottom:12px; }

/* 资讯列表 tab 内布局 */
.tab-empty { text-align:center; padding:60px 0; }
.article-layout { display:grid; grid-template-columns:320px 1fr; gap:14px; height:calc(100vh - 260px); min-height:540px; }
.list-panel { border:1px solid #ebeef5; border-radius:8px; display:flex; flex-direction:column; overflow:hidden; height:100%; }
.list-hd    { display:flex; gap:6px; padding:10px; border-bottom:1px solid #f0f0f0; flex-shrink:0; }
.news-list  { flex:1; overflow-y:auto; padding:6px; display:flex; flex-direction:column; gap:5px; }
.list-empty { text-align:center; padding:40px 0; font-size:12px; color:#c0c4cc; }
.detail-right { border:1px solid #ebeef5; border-radius:8px; overflow-y:auto; padding:14px; height:100%; box-sizing:border-box; }

/* 资讯卡片 */
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

/* AI 对比 */
.compare-layout { display:grid; grid-template-columns:1fr 1fr; gap:14px; align-items:start; }
.orig-panel { border:1px solid #ebeef5; border-radius:8px; padding:14px; }
.panel-hd { font-size:12px; font-weight:600; color:#909399; margin-bottom:8px; }
.orig-title { font-size:14px; font-weight:700; color:#1a1a1a; line-height:1.5; margin-bottom:8px; }
.orig-meta  { display:flex; align-items:center; gap:6px; margin-bottom:10px; }
.meta-source{ font-size:11px; color:#909399; }
.meta-time  { font-size:11px; color:#c0c4cc; }
.orig-content { font-size:12px; color:#606266; line-height:1.8; }
.ai-results { display:flex; flex-direction:column; gap:10px; }
.ai-card { border-radius:8px; padding:12px; border:1.5px solid #ebeef5; }
.ai-card.done  { border-color:#67c23a; background:#fafffe; }
.ai-card.empty { background:#fafafa; }
.ai-card-hd { display:flex; align-items:center; gap:6px; margin-bottom:8px; }
.ai-status  { font-size:11px; color:#909399; flex:1; }
.fn-chip    { font-size:11px; font-weight:700; padding:2px 8px; border-radius:10px; letter-spacing:.5px; }
.fn-chip.blue   { background:#ecf5ff; color:#409eff; }
.fn-chip.green  { background:#f0f9eb; color:#67c23a; }
.fn-chip.orange { background:#fdf6ec; color:#e6a23c; }
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
.sql-mini pre { background:#f5f7fa; border-radius:6px; padding:8px 12px; font-size:10px; color:#606266; font-family:monospace; line-height:1.7; margin:0; white-space:pre; overflow-x:auto; }

/* 指标大盘 */
.kpi-row   { display:grid; grid-template-columns:repeat(5,1fr); gap:12px; margin-bottom:4px; }
.kpi-card  { background:#f9fafc; border-radius:8px; padding:14px 16px; text-align:center; }
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
.signal-list { display:flex; flex-direction:column; gap:8px; max-height:400px; overflow-y:auto; }
.signal-card { border-radius:8px; padding:12px 14px; border:1.5px solid #ebeef5; }
.signal-card.bullish { border-color:#f56c6c; background:#fff9f9; }
.signal-card.bearish { border-color:#67c23a; background:#f9fff9; }
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
.company-list { display:flex; flex-direction:column; gap:5px; max-height:300px; overflow-y:auto; }
.co-row  { display:flex; align-items:center; gap:6px; font-size:12px; padding:4px 0; border-bottom:1px solid #f5f5f5; }
.co-name { font-weight:600; color:#303133; width:80px; flex-shrink:0; }
.co-tags { display:flex; flex-wrap:wrap; flex:1; }
.co-cnt  { color:#909399; font-size:11px; flex-shrink:0; }

/* 函数说明 */
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
.doc-sql pre { background:#1e1e2e; color:#a8b3cf; font-size:10px; border-radius:6px; padding:8px 10px; white-space:pre; overflow-x:auto; line-height:1.7; margin:0 0 10px; }
.doc-use   { font-size:11px; color:#909399; margin-bottom:8px; }
.doc-stat  { font-size:12px; color:#606266; background:#f5f7fa; padding:5px 10px; border-radius:4px; }
.arch-explain { background:#f9fafc; border-radius:8px; padding:16px; }
.ae-title  { font-size:13px; font-weight:700; color:#1a1a1a; margin-bottom:14px; }
.ae-grid   { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; }
.ae-item   { background:#fff; border-radius:8px; padding:12px; text-align:center; box-shadow:0 1px 4px rgba(0,0,0,.05); }
.ae-icon   { font-size:24px; margin-bottom:6px; }
.ae-label  { font-size:12px; font-weight:600; color:#303133; margin-bottom:4px; }
.ae-desc   { font-size:11px; color:#909399; line-height:1.5; }

/* 通用 */
.two-col   { display:grid; grid-template-columns:1fr 1fr; gap:14px; }
.ct        { font-size:13px; font-weight:600; color:#303133; margin-bottom:8px; }
.chart-h220{ height:220px; }
.chart-h240{ height:240px; }
.chart-h260{ height:260px; }
.chart-h300{ height:300px; }
.grey      { color:#c0c4cc; }
.empty-tip { text-align:center; padding:40px 0; font-size:13px; color:#c0c4cc; }
.sql-card  { background:#1e1e2e; border-radius:8px; padding:10px 14px; }
.sql-title { font-size:11px; color:#67c23a; font-weight:600; margin-bottom:6px; }
.sql-code  { color:#a8b3cf; font-size:10px; font-family:monospace; white-space:pre; margin:0; line-height:1.7; overflow-x:auto; }
</style>
