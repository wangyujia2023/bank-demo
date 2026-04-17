<template>
  <div>
    <el-tabs v-model="activeTab" type="card" style="margin-bottom:0">
      <el-tab-pane label="🎯 人群包构建" name="crowd" />
      <el-tab-pane label="📊 人群画像" name="portrait" />
      <el-tab-pane label="🗺️ 地图投放" name="map" />
      <el-tab-pane label="🔬 标签分析" name="taganalysis" />
      <el-tab-pane label="📋 宽表查询" name="wide" />
      <el-tab-pane label="📈 行为分析" name="behavior" />
      <el-tab-pane label="🔄 ETL" name="etl" />
    </el-tabs>

    <!-- ═══════════════════ 人群包构建 ═══════════════════ -->
    <div v-if="activeTab === 'crowd'" class="card" style="border-top-left-radius:0">
      <el-row :gutter="16">
        <!-- 左侧：标签选择 -->
        <el-col :span="14">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
            <span style="font-size:13px;font-weight:600">标签圈选（左键=包含，右键=排除）</span>
            <div style="display:flex;gap:8px">
              <el-tag type="success" size="small">包含 {{ includeTagIds.length }}</el-tag>
              <el-tag type="danger" size="small">排除 {{ excludeTagIds.length }}</el-tag>
            </div>
          </div>
          <div v-for="grp in tagMeta" :key="grp.category" style="margin-bottom:12px">
            <div style="font-size:11px;color:#909399;margin-bottom:5px;text-transform:uppercase;letter-spacing:.5px">{{ grp.category }}</div>
            <div style="display:flex;flex-wrap:wrap;gap:6px">
              <div
                v-for="t in grp.tags" :key="t.tag_id"
                @click="toggleInclude(t.tag_id)"
                @contextmenu.prevent="toggleExclude(t.tag_id)"
                :class="tagClass(t.tag_id)"
                class="tag-chip"
              >{{ t.label }}</div>
            </div>
          </div>
        </el-col>

        <!-- 右侧：当前圈选 + 操作 -->
        <el-col :span="10">
          <div style="background:#f5f7fa;border-radius:6px;padding:14px">
            <div style="font-size:13px;font-weight:600;margin-bottom:10px">人群预估</div>
            <div style="margin-bottom:8px">
              <div style="font-size:12px;color:#67c23a;margin-bottom:4px">✓ 包含</div>
              <el-tag
                v-for="tid in includeTagIds" :key="tid" type="success" closable size="small"
                @close="includeTagIds = includeTagIds.filter(x=>x!==tid)"
                style="margin:2px"
              >{{ tagLabel(tid) }}</el-tag>
              <span v-if="!includeTagIds.length" style="font-size:12px;color:#c0c4cc">（不限）</span>
            </div>
            <div style="margin-bottom:12px">
              <div style="font-size:12px;color:#f56c6c;margin-bottom:4px">✗ 排除</div>
              <el-tag
                v-for="tid in excludeTagIds" :key="tid" type="danger" closable size="small"
                @close="excludeTagIds = excludeTagIds.filter(x=>x!==tid)"
                style="margin:2px"
              >{{ tagLabel(tid) }}</el-tag>
              <span v-if="!excludeTagIds.length" style="font-size:12px;color:#c0c4cc">（无）</span>
            </div>

            <el-button type="primary" size="small" :loading="bitmapLoading" @click="computeBitmapCrowd" style="width:100%;margin-bottom:10px">
              Bitmap 估算人群规模
            </el-button>

            <div v-if="crowdSize !== null" style="text-align:center;margin-bottom:12px">
              <div style="font-size:28px;font-weight:700;color:#409eff">{{ crowdSize.toLocaleString() }}</div>
              <div style="font-size:12px;color:#909399">预计覆盖用户数</div>
            </div>

            <el-divider />
            <div style="font-size:13px;font-weight:600;margin-bottom:8px">保存人群包</div>
            <el-input v-model="crowdName" placeholder="人群包名称" size="small" style="margin-bottom:6px" />
            <el-input v-model="crowdDesc" placeholder="描述（可选）" size="small" style="margin-bottom:8px" />
            <el-button type="success" size="small" :disabled="!crowdName || crowdSize === null" @click="saveCrowd" style="width:100%">
              保存人群包
            </el-button>
          </div>
        </el-col>
      </el-row>

      <!-- 人群包列表 -->
      <el-divider>已保存人群包</el-divider>
      <el-row :gutter="12" style="margin-bottom:10px;align-items:center">
        <el-col :span="20">
          <el-checkbox-group v-model="compareIds" :max="2" style="display:inline">
            <el-checkbox v-for="pkg in crowdList" :key="pkg.crowd_id" :value="pkg.crowd_id" style="margin:0 8px 8px 0">
              <el-card shadow="hover" style="width:220px;display:inline-block;vertical-align:top;cursor:pointer">
                <div style="font-size:13px;font-weight:600">{{ pkg.name }}</div>
                <div style="font-size:11px;color:#909399;margin:3px 0">{{ pkg.desc || '—' }}</div>
                <div style="font-size:12px;color:#409eff">{{ pkg.crowd_size.toLocaleString() }} 人</div>
                <div style="font-size:11px;color:#c0c4cc;margin-top:4px">{{ pkg.created_at }}</div>
                <el-button size="small" type="danger" text @click.stop="deleteCrowd(pkg.crowd_id)" style="position:absolute;top:6px;right:6px">删除</el-button>
              </el-card>
            </el-checkbox>
          </el-checkbox-group>
        </el-col>
        <el-col :span="4">
          <el-button type="primary" :disabled="compareIds.length !== 2" :loading="compareLoading" @click="runCompare">
            对比分析
          </el-button>
        </el-col>
      </el-row>

      <!-- 对比结果 -->
      <div v-if="compareResult">
        <el-divider>人群包对比：{{ compareResult.pkg_a.name }} vs {{ compareResult.pkg_b.name }}</el-divider>
        <el-table :data="compareResult.diffs.slice(0,20)" border size="small">
          <el-table-column prop="label" label="标签" width="140" />
          <el-table-column prop="category" label="分类" width="80" />
          <el-table-column label="包A命中率" width="160">
            <template #default="{row}">
              <el-progress :percentage="Math.min(row.pct_a,100)" :stroke-width="8" :format="()=>row.pct_a+'%'" />
            </template>
          </el-table-column>
          <el-table-column label="包B命中率" width="160">
            <template #default="{row}">
              <el-progress :percentage="Math.min(row.pct_b,100)" :stroke-width="8" :format="()=>row.pct_b+'%'" />
            </template>
          </el-table-column>
          <el-table-column label="差值" width="90">
            <template #default="{row}">
              <span :style="{color: row.diff>0?'#67c23a':row.diff<0?'#f56c6c':'#909399'}">
                {{ row.diff > 0 ? '+' : '' }}{{ row.diff }}%
              </span>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </div>

    <!-- ═══════════════════ 人群画像 ═══════════════════ -->
    <div v-if="activeTab === 'portrait'" class="card" style="border-top-left-radius:0">
      <el-tabs v-model="portraitTab" type="border-card">
        <!-- TGI -->
        <el-tab-pane label="TGI 指数分析" name="tgi">
          <div style="margin:10px 0;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <span style="font-size:13px">目标人群标签：</span>
            <el-select v-model="tgiTagIds" multiple collapse-tags collapse-tags-tooltip placeholder="（可选）不选则分析全体" style="width:320px">
              <el-option-group v-for="grp in tagMeta" :key="grp.category" :label="grp.category">
                <el-option v-for="t in grp.tags" :key="t.tag_id" :label="t.label" :value="t.tag_id" />
              </el-option-group>
            </el-select>
            <el-button type="primary" :loading="tgiLoading" @click="runTgi">分析</el-button>
          </div>
          <div v-if="tgiData.length">
            <el-select v-model="tgiCat" placeholder="按分类筛选" clearable size="small" style="margin-bottom:10px;width:160px">
              <el-option v-for="c in tgiCats" :key="c" :label="c" :value="c" />
            </el-select>
            <v-chart :option="tgiOption" style="height:420px" autoresize />
            <div style="font-size:12px;color:#909399;margin-top:6px">
              TGI = (目标人群占比 / 全体基准占比) × 100，＞100 代表该标签在目标人群中超指数分布
            </div>
          </div>
        </el-tab-pane>

        <!-- 交叉分析 -->
        <el-tab-pane label="交叉分析" name="cross">
          <div style="margin:10px 0;display:flex;gap:12px;align-items:center">
            <el-select v-model="crossCat1" placeholder="分类 A" style="width:150px">
              <el-option v-for="c in allCats" :key="c" :label="c" :value="c" />
            </el-select>
            <span>×</span>
            <el-select v-model="crossCat2" placeholder="分类 B" style="width:150px">
              <el-option v-for="c in allCats" :key="c" :label="c" :value="c" />
            </el-select>
            <el-button type="primary" :loading="crossLoading" @click="runCross">交叉分析</el-button>
          </div>
          <div v-if="crossData">
            <div style="overflow-x:auto">
              <table class="cross-matrix">
                <thead>
                  <tr>
                    <th>{{ crossCat1 }} \ {{ crossCat2 }}</th>
                    <th v-for="l in crossData.tags2" :key="l">{{ l }}</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in crossData.matrix" :key="row.label">
                    <td style="font-weight:600;background:#f5f7fa">{{ row.label }}</td>
                    <td
                      v-for="cell in row.cells" :key="cell"
                      :style="{ background: crossColor(cell.pct, crossMaxPct) }"
                      :title="`${cell.count} 人 / ${cell.pct}%`"
                    >{{ cell.pct }}%</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div style="font-size:12px;color:#909399;margin-top:6px">总用户 {{ crossData.total.toLocaleString() }}，颜色深度代表共现比例</div>
          </div>
        </el-tab-pane>

        <!-- 地域分布 -->
        <el-tab-pane label="地域分布" name="geo">
          <div style="margin:10px 0;display:flex;gap:8px;align-items:center">
            <span style="font-size:13px">圈选标签（可选）：</span>
            <el-select v-model="geoTagIds" multiple collapse-tags placeholder="不选=全体" style="width:280px">
              <el-option-group v-for="grp in tagMeta" :key="grp.category" :label="grp.category">
                <el-option v-for="t in grp.tags" :key="t.tag_id" :label="t.label" :value="t.tag_id" />
              </el-option-group>
            </el-select>
            <el-button type="primary" :loading="geoLoading" @click="runGeo">查询</el-button>
          </div>
          <div v-if="geoData">
            <el-row :gutter="16">
              <el-col :span="12">
                <v-chart :option="geoProvinceOption" style="height:380px" autoresize />
              </el-col>
              <el-col :span="12">
                <el-table :data="geoData.province_list.slice(0,15)" border size="small" max-height="360">
                  <el-table-column prop="province" label="省份" />
                  <el-table-column label="用户数">
                    <template #default="{row}">
                      <el-progress :percentage="Math.round(row.cnt/geoData.province_list[0].cnt*100)" :stroke-width="8" :format="()=>row.cnt.toLocaleString()" />
                    </template>
                  </el-table-column>
                </el-table>
              </el-col>
            </el-row>
          </div>
        </el-tab-pane>
      </el-tabs>
    </div>

    <!-- ═══════════════════ 地图投放 ═══════════════════ -->
    <div v-if="activeTab === 'map'" class="card" style="border-top-left-radius:0">
      <el-row :gutter="20">
        <el-col :span="14">
          <div style="font-size:13px;font-weight:600;margin-bottom:10px">省份圈选（点击选择投放地域）</div>
          <div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px">
            <div
              v-for="p in provinces" :key="p"
              @click="toggleProvince(p)"
              :class="['province-chip', selectedProvinces.includes(p) ? 'selected' : '']"
            >{{ p }}</div>
          </div>
          <div style="margin-bottom:12px">
            <el-tag v-if="!selectedProvinces.length" type="info" size="small">未选择省份</el-tag>
            <el-tag v-for="p in selectedProvinces" :key="p" closable @close="selectedProvinces = selectedProvinces.filter(x=>x!==p)" style="margin:2px">{{ p }}</el-tag>
          </div>
          <el-button type="primary" :loading="mapLoading" :disabled="!selectedProvinces.length" @click="runTargeting">
            估算投放覆盖
          </el-button>

          <div v-if="targetingData" style="margin-top:16px">
            <el-row :gutter="12" style="margin-bottom:12px">
              <el-col :span="8">
                <div class="stat-card">
                  <div class="stat-label">覆盖用户</div>
                  <div class="stat-value" style="font-size:22px">{{ targetingData.total.toLocaleString() }}</div>
                </div>
              </el-col>
            </el-row>
            <el-table :data="targetingData.detail" border size="small">
              <el-table-column prop="province" label="省份" width="100" />
              <el-table-column label="用户数" prop="cnt">
                <template #default="{row}">{{ (row.cnt||0).toLocaleString() }}</template>
              </el-table-column>
              <el-table-column label="平均AUM" prop="avg_aum">
                <template #default="{row}">¥{{ Number(row.avg_aum||0).toLocaleString() }}</template>
              </el-table-column>
              <el-table-column label="异常用户" prop="anomaly_cnt">
                <template #default="{row}">
                  <el-tag :type="row.anomaly_cnt>0?'danger':'success'" size="small">{{ row.anomaly_cnt||0 }}</el-tag>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </el-col>

        <el-col :span="10">
          <div style="background:#f5f7fa;border-radius:6px;padding:16px">
            <div style="font-size:13px;font-weight:600;margin-bottom:12px">投放策略配置</div>
            <el-form label-width="90px" size="small">
              <el-form-item label="投放渠道">
                <el-checkbox-group v-model="strategy.channels">
                  <el-checkbox value="APP">APP 推送</el-checkbox>
                  <el-checkbox value="SMS">短信</el-checkbox>
                  <el-checkbox value="EMAIL">邮件</el-checkbox>
                  <el-checkbox value="WECHAT">微信</el-checkbox>
                </el-checkbox-group>
              </el-form-item>
              <el-form-item label="投放时段">
                <el-time-picker v-model="strategy.timeStart" placeholder="开始" style="width:100px" format="HH:mm" />
                <span style="margin:0 6px">—</span>
                <el-time-picker v-model="strategy.timeEnd" placeholder="结束" style="width:100px" format="HH:mm" />
              </el-form-item>
              <el-form-item label="频次上限">
                <el-input-number v-model="strategy.freqCap" :min="1" :max="10" />
                <span style="margin-left:6px;font-size:12px;color:#909399">次/天</span>
              </el-form-item>
              <el-form-item label="活动主题">
                <el-input v-model="strategy.theme" placeholder="如：理财产品推荐" />
              </el-form-item>
              <el-form-item>
                <el-button type="primary" @click="genStrategy">生成投放方案</el-button>
              </el-form-item>
            </el-form>
            <div v-if="strategyPlan" style="margin-top:8px;padding:10px;background:#fff;border-radius:4px;font-size:12px;line-height:1.8;color:#303133">
              <div v-for="line in strategyPlan" :key="line">{{ line }}</div>
            </div>
          </div>
        </el-col>
      </el-row>
    </div>

    <!-- ═══════════════════ 标签分析 ═══════════════════ -->
    <div v-if="activeTab === 'taganalysis'" class="card" style="border-top-left-radius:0">
      <el-tabs v-model="taTab" type="border-card">
        <!-- 标签权重 -->
        <el-tab-pane label="标签权重分析" name="weight">
          <div style="margin:10px 0;display:flex;gap:8px;align-items:center">
            <el-select v-model="weightTagIds" multiple collapse-tags placeholder="选择目标人群标签" style="width:320px">
              <el-option-group v-for="grp in tagMeta" :key="grp.category" :label="grp.category">
                <el-option v-for="t in grp.tags" :key="t.tag_id" :label="t.label" :value="t.tag_id" />
              </el-option-group>
            </el-select>
            <el-button type="primary" :loading="weightLoading" @click="runWeight">分析</el-button>
          </div>
          <div v-if="weightData">
            <el-row :gutter="16">
              <el-col v-for="cat in weightData.categories.slice(0,6)" :key="cat.category" :span="12" style="margin-bottom:16px">
                <div style="font-size:12px;font-weight:600;margin-bottom:6px">
                  {{ cat.category }}
                  <el-tag size="small" style="margin-left:6px">均值TGI {{ cat.avg_tgi }}</el-tag>
                </div>
                <div v-for="item in cat.tags.slice(0,5)" :key="item.tag_id" style="display:flex;align-items:center;gap:8px;margin-bottom:3px;font-size:12px">
                  <span style="width:80px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{{ item.label }}</span>
                  <el-progress
                    :percentage="Math.min(item.tgi, 200)"
                    :stroke-width="8"
                    :color="item.tgi > 120 ? '#67c23a' : item.tgi > 80 ? '#409eff' : '#f56c6c'"
                    style="flex:1"
                    :format="() => item.tgi"
                  />
                </div>
              </el-col>
            </el-row>
          </div>
        </el-tab-pane>

        <!-- 异常检测 -->
        <el-tab-pane label="异常用户检测" name="anomaly">
          <div style="margin:10px 0">
            <el-button type="warning" :loading="anomalyLoading" @click="runAnomaly">分析异常用户画像</el-button>
          </div>
          <div v-if="anomalyData">
            <el-row :gutter="12" style="margin-bottom:16px">
              <el-col :span="6">
                <div class="stat-card">
                  <div class="stat-label">异常用户数</div>
                  <div class="stat-value" style="font-size:22px;color:#f56c6c">{{ anomalyData.anomaly_count.toLocaleString() }}</div>
                </div>
              </el-col>
              <el-col :span="6">
                <div class="stat-card">
                  <div class="stat-label">总用户数</div>
                  <div class="stat-value" style="font-size:22px">{{ anomalyData.total_users.toLocaleString() }}</div>
                </div>
              </el-col>
            </el-row>
            <el-table :data="anomalyData.tags.slice(0,20)" border size="small">
              <el-table-column prop="label" label="标签" width="140" />
              <el-table-column prop="category" label="分类" width="80" />
              <el-table-column label="全体占比">
                <template #default="{row}">
                  <el-progress :percentage="Math.min(row.base_pct,100)" :stroke-width="8" :format="()=>row.base_pct+'%'" />
                </template>
              </el-table-column>
              <el-table-column label="异常用户占比">
                <template #default="{row}">
                  <el-progress :percentage="Math.min(row.anom_pct,100)" :stroke-width="8" :color="'#f56c6c'" :format="()=>row.anom_pct+'%'" />
                </template>
              </el-table-column>
              <el-table-column label="TGI" width="90">
                <template #default="{row}">
                  <el-tag :type="row.tgi>120?'danger':row.tgi>80?'warning':'info'" size="small">{{ row.tgi }}</el-tag>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </el-tab-pane>
      </el-tabs>
    </div>

    <!-- ═══════════════════ 宽表查询 ═══════════════════ -->
    <div v-if="activeTab === 'wide'" class="card" style="border-top-left-radius:0">
      <el-row :gutter="16">
        <el-col :span="16">
          <div style="font-size:13px;font-weight:600;margin-bottom:10px">
            选择标签（多选为 AND 交集）
          </div>
          <div v-for="grp in tagMeta" :key="grp.category" style="margin-bottom:12px">
            <div style="font-size:12px;color:#909399;margin-bottom:6px">{{ grp.category }}</div>
            <el-checkbox-group v-model="selectedTagIds" style="display:flex;flex-wrap:wrap;gap:6px">
              <el-checkbox
                v-for="t in grp.tags" :key="t.tag_id"
                :value="t.tag_id" border size="small"
              >{{ t.label }}</el-checkbox>
            </el-checkbox-group>
          </div>
        </el-col>
        <el-col :span="8">
          <div style="padding:12px;background:#f5f7fa;border-radius:6px">
            <div style="font-size:13px;font-weight:600;margin-bottom:10px">已选标签</div>
            <el-tag
              v-for="tid in selectedTagIds" :key="tid" closable
              @close="selectedTagIds = selectedTagIds.filter(x => x !== tid)"
              style="margin:3px"
            >{{ tagLabel(tid) }}</el-tag>
            <div v-if="!selectedTagIds.length" style="color:#c0c4cc;font-size:12px">未选择（查全部）</div>
            <el-divider />
            <el-button type="primary" style="width:100%" :loading="wideLoading" @click="queryWide">查询</el-button>
          </div>
          <div style="margin-top:16px">
            <div style="font-size:13px;font-weight:600;margin-bottom:8px">标签命中用户数（TOP 15）</div>
            <div v-for="item in distribution.slice(0,15)" :key="item.col" style="display:flex;align-items:center;gap:8px;margin-bottom:4px;font-size:12px">
              <span style="width:90px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{{ item.label }}</span>
              <el-progress :percentage="distPercent(item.count)" :stroke-width="10" style="flex:1" :format="() => item.count.toLocaleString()" />
            </div>
            <el-button size="small" plain @click="loadDistribution" style="margin-top:6px">刷新分布</el-button>
          </div>
        </el-col>
      </el-row>
      <div v-if="wideResult" style="margin-top:16px">
        <div style="margin-bottom:8px;font-size:13px;color:#606266">
          共 <b>{{ wideResult.total.toLocaleString() }}</b> 条，第 {{ wideResult.page }} 页
        </div>
        <el-table :data="wideResult.rows" border size="small" style="width:100%">
          <el-table-column prop="customer_id" label="用户ID" width="100" />
          <el-table-column label="标签" min-width="300">
            <template #default="{row}">
              <el-tag v-for="tag in row.active_tags" :key="tag" size="small" type="success" style="margin:2px">{{ tag }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="update_time" label="更新时间" width="160" />
        </el-table>
        <el-pagination
          v-if="wideResult.total > wideResult.page_size"
          style="margin-top:10px"
          layout="prev, pager, next"
          :total="wideResult.total"
          :page-size="wideResult.page_size"
          :current-page="widePage"
          @current-change="onWidePage"
        />
        <div style="font-size:12px;color:#909399;margin-top:8px">
          Doris <code>DUPLICATE KEY</code> 宽表 · TINYINT 0/1 · 向量化扫描
        </div>
      </div>
    </div>

    <!-- ═══════════════════ 行为分析 ═══════════════════ -->
    <div v-if="activeTab === 'behavior'" class="card" style="border-top-left-radius:0">
      <el-tabs v-model="behaviorTab" type="border-card">
        <el-tab-pane label="漏斗分析 window_funnel()" name="funnel">
          <el-form inline style="margin-top:8px">
            <el-form-item label="窗口(秒)">
              <el-input-number v-model="funnelWindow" :min="3600" :max="2592000" :step="86400" style="width:130px" />
            </el-form-item>
            <el-form-item label="人群过滤">
              <el-select v-model="funnelFilterTags" multiple collapse-tags placeholder="（可选）" style="width:200px">
                <el-option-group v-for="grp in tagMeta" :key="grp.category" :label="grp.category">
                  <el-option v-for="t in grp.tags" :key="t.tag_id" :label="t.label" :value="t.tag_id" />
                </el-option-group>
              </el-select>
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="funnelLoading" @click="runFunnel">分析</el-button>
            </el-form-item>
          </el-form>
          <div style="margin-bottom:12px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
            <span style="font-size:13px">漏斗步骤：</span>
            <el-tag v-for="(s, i) in funnelSteps" :key="i" closable type="primary" @close="funnelSteps.splice(i,1)" size="small">{{ i+1 }}. {{ s }}</el-tag>
            <el-select v-model="newStep" placeholder="添加步骤" size="small" style="width:160px" @change="addStep">
              <el-option v-for="e in eventTypes" :key="e" :label="e" :value="e" />
            </el-select>
          </div>
          <div v-if="funnelData">
            <v-chart :option="funnelOption" style="height:300px" autoresize />
            <el-table :data="funnelData.steps" border size="small" style="margin-top:10px">
              <el-table-column prop="step" label="步骤" width="60" />
              <el-table-column prop="step_name" label="事件" />
              <el-table-column prop="user_count" label="用户数">
                <template #default="{row}">{{ (row.user_count||0).toLocaleString() }}</template>
              </el-table-column>
              <el-table-column prop="conversion_rate" label="环节转化率">
                <template #default="{row}"><el-progress :percentage="row.conversion_rate||0" :stroke-width="8" /></template>
              </el-table-column>
              <el-table-column prop="overall_rate" label="总转化率">
                <template #default="{row}"><el-tag size="small">{{ row.overall_rate }}%</el-tag></template>
              </el-table-column>
            </el-table>
          </div>
        </el-tab-pane>

        <el-tab-pane label="留存分析 retention()" name="retention">
          <el-form inline style="margin-top:8px">
            <el-form-item label="起始事件">
              <el-select v-model="retCohort" style="width:130px">
                <el-option v-for="e in eventTypes" :key="e" :label="e" :value="e" />
              </el-select>
            </el-form-item>
            <el-form-item label="回访事件">
              <el-select v-model="retReturn" style="width:130px">
                <el-option v-for="e in eventTypes" :key="e" :label="e" :value="e" />
              </el-select>
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="retLoading" @click="runRetention">分析</el-button>
            </el-form-item>
          </el-form>
          <div v-if="retentionData" style="overflow-x:auto;margin-top:12px">
            <table class="retention-matrix">
              <thead>
                <tr>
                  <th>队列日期</th><th>初始用户</th>
                  <th v-for="d in retentionData.retention_days" :key="d">第{{ d }}天</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in retentionData.rows" :key="row.cohort_date">
                  <td>{{ row.cohort_date }}</td>
                  <td>{{ (row.cohort_size||0).toLocaleString() }}</td>
                  <td v-for="d in retentionData.retention_days" :key="d" :style="{ background: heatColor(row[`d${d}_rate`]) }">
                    {{ row[`d${d}_rate`] }}%
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </el-tab-pane>

        <el-tab-pane label="行为路径分析" name="path">
          <el-button type="primary" :loading="pathLoading" @click="runPath" style="margin-top:8px">分析 Top 10 路径</el-button>
          <div v-if="pathData" style="margin-top:16px">
            <v-chart :option="pathOption" style="height:360px" autoresize />
          </div>
        </el-tab-pane>
      </el-tabs>
    </div>

    <!-- ═══════════════════ ETL ═══════════════════ -->
    <div v-if="activeTab === 'etl'" class="card" style="border-top-left-radius:0">
      <el-row :gutter="20">
        <el-col :span="10">
          <div class="card-title">宽表 → 高表 ETL</div>
          <p style="font-size:13px;color:#606266;line-height:1.8">
            将 <code>bank.user_tag_wide</code> 各 TINYINT 标签列<br/>
            通过 <b>BITMAP_UNION(TO_BITMAP(customer_id))</b><br/>
            写入 <code>bank.t_customer_tags</code>（高表）
          </p>
          <el-button type="primary" :loading="etlLoading" @click="runEtl" style="margin-top:8px">执行 ETL 同步</el-button>
          <div v-if="etlResult" style="margin-top:12px">
            <el-alert
              :title="etlResult.success ? `同步完成：高表共 ${etlResult.tag_rows} 行` : `ETL 失败：${etlResult.message}`"
              :type="etlResult.success ? 'success' : 'error'"
              show-icon :closable="false"
            />
          </div>
        </el-col>
        <el-col :span="14">
          <div class="card-title">高表标签 Bitmap 用户数</div>
          <el-button size="small" plain @click="loadEtlOverview" style="margin-bottom:10px">刷新</el-button>
          <el-table :data="etlOverview" border size="small" max-height="420">
            <el-table-column prop="tag_id" label="tag_id" width="75" />
            <el-table-column prop="tag_name" label="列名" width="170" />
            <el-table-column prop="label" label="标签名" width="110" />
            <el-table-column prop="category" label="分类" width="80" />
            <el-table-column prop="user_count" label="命中用户数">
              <template #default="{row}">{{ (row.user_count || 0).toLocaleString() }}</template>
            </el-table-column>
          </el-table>
        </el-col>
      </el-row>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { use } from 'echarts/core'
import { FunnelChart, BarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'
import axios from 'axios'

use([FunnelChart, BarChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer])

const BASE = '/api'
const activeTab = ref('crowd')

// ── 标签元数据 ──
const tagMeta = ref([])
const allCats = computed(() => tagMeta.value.map(g => g.category))

async function loadTagMeta() {
  const { data } = await axios.get(`${BASE}/cdp/wide/tag-meta`)
  tagMeta.value = data
}
function tagLabel(tid) {
  for (const grp of tagMeta.value) {
    const t = grp.tags.find(x => x.tag_id === tid)
    if (t) return t.label
  }
  return String(tid)
}

// ══════════════════════════════════════════
// 人群包构建
// ══════════════════════════════════════════
const includeTagIds  = ref([])
const excludeTagIds  = ref([])
const bitmapLoading  = ref(false)
const crowdSize      = ref(null)
const crowdName      = ref('')
const crowdDesc      = ref('')
const crowdList      = ref([])
const compareIds     = ref([])
const compareLoading = ref(false)
const compareResult  = ref(null)

function tagClass(tid) {
  if (includeTagIds.value.includes(tid)) return 'chip-include'
  if (excludeTagIds.value.includes(tid)) return 'chip-exclude'
  return 'chip-default'
}
function toggleInclude(tid) {
  excludeTagIds.value = excludeTagIds.value.filter(x => x !== tid)
  if (includeTagIds.value.includes(tid)) {
    includeTagIds.value = includeTagIds.value.filter(x => x !== tid)
  } else {
    includeTagIds.value.push(tid)
  }
  crowdSize.value = null
}
function toggleExclude(tid) {
  includeTagIds.value = includeTagIds.value.filter(x => x !== tid)
  if (excludeTagIds.value.includes(tid)) {
    excludeTagIds.value = excludeTagIds.value.filter(x => x !== tid)
  } else {
    excludeTagIds.value.push(tid)
  }
  crowdSize.value = null
}

async function computeBitmapCrowd() {
  bitmapLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/bitmap/compute`, {
      include_tag_ids: includeTagIds.value,
      exclude_tag_ids: excludeTagIds.value,
    })
    crowdSize.value = data.crowd_size
  } finally {
    bitmapLoading.value = false
  }
}

async function saveCrowd() {
  const { data } = await axios.post(`${BASE}/cdp/crowd/save`, {
    name: crowdName.value,
    desc: crowdDesc.value,
    include_tag_ids: includeTagIds.value,
    exclude_tag_ids: excludeTagIds.value,
    crowd_size: crowdSize.value,
  })
  crowdList.value.unshift(data)
  crowdName.value = ''
  crowdDesc.value = ''
}

async function deleteCrowd(id) {
  await axios.delete(`${BASE}/cdp/crowd/${id}`)
  crowdList.value = crowdList.value.filter(p => p.crowd_id !== id)
  compareIds.value = compareIds.value.filter(x => x !== id)
}

async function loadCrowdList() {
  const { data } = await axios.get(`${BASE}/cdp/crowd/list`)
  crowdList.value = data
}

async function runCompare() {
  if (compareIds.value.length !== 2) return
  compareLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/crowd/compare`, {
      id_a: compareIds.value[0],
      id_b: compareIds.value[1],
    })
    compareResult.value = data
  } finally {
    compareLoading.value = false
  }
}

// ══════════════════════════════════════════
// 人群画像
// ══════════════════════════════════════════
const portraitTab = ref('tgi')

// TGI
const tgiTagIds  = ref([])
const tgiLoading = ref(false)
const tgiData    = ref([])
const tgiCat     = ref('')
const tgiCats    = computed(() => [...new Set(tgiData.value.map(x => x.category))])

async function runTgi() {
  tgiLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/portrait/tgi`, { include_tag_ids: tgiTagIds.value })
    tgiData.value = data
  } finally {
    tgiLoading.value = false
  }
}

const tgiOption = computed(() => {
  let rows = tgiData.value
  if (tgiCat.value) rows = rows.filter(x => x.category === tgiCat.value)
  rows = rows.slice(0, 20).reverse()
  return {
    tooltip: { trigger: 'axis', formatter: params => {
      const d = params[0]
      return `${d.name}<br/>TGI: <b>${d.value}</b><br/>目标: ${rows[d.dataIndex]?.seg_pct}% / 基准: ${rows[d.dataIndex]?.base_pct}%`
    }},
    grid: { left: '28%', right: '10%', top: '4%', bottom: '4%' },
    xAxis: { type: 'value', axisLabel: { fontSize: 11 } },
    yAxis: { type: 'category', data: rows.map(r => r.label), axisLabel: { fontSize: 11 } },
    series: [{
      type: 'bar',
      data: rows.map(r => ({
        value: r.tgi,
        itemStyle: { color: r.tgi >= 120 ? '#67c23a' : r.tgi >= 80 ? '#409eff' : '#f56c6c' }
      })),
      markLine: { data: [{ xAxis: 100, lineStyle: { color: '#e6a23c', type: 'dashed' } }] },
      label: { show: true, position: 'right', fontSize: 11 },
    }],
  }
})

// 交叉分析
const crossCat1   = ref('')
const crossCat2   = ref('')
const crossLoading = ref(false)
const crossData   = ref(null)
const crossMaxPct = computed(() => crossData.value
  ? Math.max(...crossData.value.matrix.flatMap(r => r.cells.map(c => c.pct)), 1)
  : 1
)
function crossColor(pct, max) {
  const r = Math.min(pct / max, 1)
  return `rgba(64,158,255,${0.1 + r * 0.7})`
}

async function runCross() {
  crossLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/portrait/cross`, { cat1: crossCat1.value, cat2: crossCat2.value })
    crossData.value = data
  } finally {
    crossLoading.value = false
  }
}

// 地域分布
const geoTagIds  = ref([])
const geoLoading = ref(false)
const geoData    = ref(null)

async function runGeo() {
  geoLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/portrait/geo`, { include_tag_ids: geoTagIds.value })
    geoData.value = data
  } finally {
    geoLoading.value = false
  }
}

const geoProvinceOption = computed(() => {
  if (!geoData.value) return {}
  const rows = [...geoData.value.province_list].slice(0, 15).reverse()
  return {
    tooltip: { trigger: 'axis' },
    grid: { left: '22%', right: '10%', top: '4%', bottom: '4%' },
    xAxis: { type: 'value' },
    yAxis: { type: 'category', data: rows.map(r => r.province), axisLabel: { fontSize: 11 } },
    series: [{ type: 'bar', data: rows.map(r => r.cnt), label: { show: true, position: 'right', fontSize: 11 } }],
  }
})

// ══════════════════════════════════════════
// 地图投放
// ══════════════════════════════════════════
const provinces = [
  '北京', '上海', '天津', '重庆',
  '广东', '浙江', '江苏', '山东', '河南', '四川', '湖北', '湖南',
  '福建', '安徽', '河北', '陕西', '云南', '辽宁', '江西', '广西',
  '黑龙江', '内蒙古', '山西', '贵州', '新疆', '吉林', '西藏', '甘肃',
  '海南', '宁夏', '青海',
]
const selectedProvinces = ref([])
const mapLoading        = ref(false)
const targetingData     = ref(null)
const strategy = ref({ channels: ['APP', 'SMS'], timeStart: null, timeEnd: null, freqCap: 3, theme: '' })
const strategyPlan      = ref(null)

function toggleProvince(p) {
  if (selectedProvinces.value.includes(p)) {
    selectedProvinces.value = selectedProvinces.value.filter(x => x !== p)
  } else {
    selectedProvinces.value.push(p)
  }
  targetingData.value = null
}

async function runTargeting() {
  mapLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/portrait/targeting`, { provinces: selectedProvinces.value })
    targetingData.value = data
  } finally {
    mapLoading.value = false
  }
}

function genStrategy() {
  const s = strategy.value
  strategyPlan.value = [
    `📍 投放地域：${selectedProvinces.value.join('、') || '未选择'}`,
    `📡 投放渠道：${s.channels.join(' + ')}`,
    `🕐 投放时段：${formatTime(s.timeStart)} — ${formatTime(s.timeEnd)}`,
    `🔁 频次上限：${s.freqCap} 次/天`,
    `🎯 活动主题：${s.theme || '—'}`,
    `👥 预计覆盖：${targetingData.value ? targetingData.value.total.toLocaleString() + ' 人' : '请先估算'}`,
  ]
}

function formatTime(d) {
  if (!d) return '--:--'
  const t = new Date(d)
  return `${String(t.getHours()).padStart(2,'0')}:${String(t.getMinutes()).padStart(2,'0')}`
}

// ══════════════════════════════════════════
// 标签分析
// ══════════════════════════════════════════
const taTab        = ref('weight')
const weightTagIds = ref([])
const weightLoading = ref(false)
const weightData   = ref(null)
const anomalyLoading = ref(false)
const anomalyData  = ref(null)

async function runWeight() {
  weightLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/tag/weight`, { include_tag_ids: weightTagIds.value })
    weightData.value = data
  } finally {
    weightLoading.value = false
  }
}

async function runAnomaly() {
  anomalyLoading.value = true
  try {
    const { data } = await axios.get(`${BASE}/cdp/tag/anomaly`)
    anomalyData.value = data
  } finally {
    anomalyLoading.value = false
  }
}

// ══════════════════════════════════════════
// 宽表查询
// ══════════════════════════════════════════
const selectedTagIds = ref([])
const wideLoading    = ref(false)
const wideResult     = ref(null)
const widePage       = ref(1)
const distribution   = ref([])
const distMax        = computed(() => Math.max(...distribution.value.map(d => d.count), 1))
const distPercent    = cnt => Math.round(cnt * 100 / distMax.value)

async function queryWide(page = 1) {
  wideLoading.value = true
  widePage.value = page
  try {
    const { data } = await axios.post(`${BASE}/cdp/wide/query`, { tag_ids: selectedTagIds.value, page, page_size: 20 })
    wideResult.value = data
  } finally {
    wideLoading.value = false
  }
}
async function loadDistribution() {
  const { data } = await axios.get(`${BASE}/cdp/wide/distribution`)
  distribution.value = data
}
function onWidePage(p) { queryWide(p) }

// ══════════════════════════════════════════
// ETL
// ══════════════════════════════════════════
const etlLoading  = ref(false)
const etlResult   = ref(null)
const etlOverview = ref([])

async function runEtl() {
  etlLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/etl/sync`)
    etlResult.value = data
    await loadEtlOverview()
  } finally {
    etlLoading.value = false
  }
}
async function loadEtlOverview() {
  const { data } = await axios.get(`${BASE}/cdp/etl/overview`)
  etlOverview.value = data
}

// ══════════════════════════════════════════
// 行为分析
// ══════════════════════════════════════════
const behaviorTab    = ref('funnel')
const eventTypes     = ['REGISTER', 'LOGIN', 'BROWSE_PRODUCT', 'APPLY', 'TRANSACTION', 'TRANSFER', 'REPAY']
const funnelSteps    = ref(['REGISTER', 'LOGIN', 'BROWSE_PRODUCT', 'APPLY', 'TRANSACTION'])
const funnelWindow   = ref(86400)
const funnelFilterTags = ref([])
const newStep        = ref('')
const funnelLoading  = ref(false)
const funnelData     = ref(null)

function addStep(v) {
  if (v && !funnelSteps.value.includes(v)) funnelSteps.value.push(v)
  newStep.value = ''
}
async function runFunnel() {
  funnelLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/behavior/funnel`, {
      steps: funnelSteps.value,
      window_seconds: funnelWindow.value,
      filter_tag_ids: funnelFilterTags.value.length ? funnelFilterTags.value : null,
    })
    funnelData.value = data
  } finally {
    funnelLoading.value = false
  }
}
const funnelOption = computed(() => {
  if (!funnelData.value) return {}
  return {
    tooltip: { trigger: 'item' },
    series: [{
      type: 'funnel', sort: 'none',
      label: { position: 'inside', formatter: '{b}: {c}人' },
      data: funnelData.value.steps.map(s => ({ name: s.step_name, value: s.user_count })),
    }],
  }
})

const retCohort     = ref('REGISTER')
const retReturn     = ref('LOGIN')
const retLoading    = ref(false)
const retentionData = ref(null)

async function runRetention() {
  retLoading.value = true
  try {
    const { data } = await axios.post(`${BASE}/cdp/behavior/retention`, {
      cohort_event: retCohort.value, return_event: retReturn.value,
    })
    retentionData.value = data
  } finally {
    retLoading.value = false
  }
}
function heatColor(rate) {
  if (!rate) return '#f5f7fa'
  const r = Math.min(rate, 100) / 100
  const g = Math.round(200 - r * 100)
  const b = Math.round(240 - r * 80)
  return `rgba(64,${g},${b},${0.3 + r * 0.5})`
}

const pathLoading = ref(false)
const pathData    = ref(null)

async function runPath() {
  pathLoading.value = true
  try {
    const { data } = await axios.get(`${BASE}/cdp/behavior/path?top_n=10`)
    pathData.value = data
  } finally {
    pathLoading.value = false
  }
}
const pathOption = computed(() => {
  if (!pathData.value) return {}
  const rows = [...pathData.value].sort((a, b) => a.freq - b.freq)
  return {
    tooltip: { trigger: 'axis' },
    grid: { left: '30%', right: '8%' },
    xAxis: { type: 'value' },
    yAxis: { type: 'category', data: rows.map(r => r.path) },
    series: [{ type: 'bar', data: rows.map(r => r.freq), label: { show: true, position: 'right' } }],
  }
})

onMounted(async () => {
  await loadTagMeta()
  await loadDistribution()
  await loadEtlOverview()
  await loadCrowdList()
})
</script>

<style scoped>
.tag-chip {
  padding: 4px 10px;
  border-radius: 4px;
  font-size: 12px;
  cursor: pointer;
  border: 1px solid #dcdfe6;
  user-select: none;
  transition: all .15s;
}
.chip-default { background: #fff; color: #606266; }
.chip-default:hover { border-color: #409eff; color: #409eff; }
.chip-include { background: #f0f9eb; border-color: #67c23a; color: #67c23a; }
.chip-exclude { background: #fef0f0; border-color: #f56c6c; color: #f56c6c; }

.province-chip {
  padding: 4px 10px;
  border-radius: 4px;
  font-size: 12px;
  cursor: pointer;
  border: 1px solid #dcdfe6;
  user-select: none;
  transition: all .15s;
  background: #fff;
  color: #606266;
}
.province-chip:hover { border-color: #409eff; color: #409eff; }
.province-chip.selected { background: #409eff; border-color: #409eff; color: #fff; }

.cross-matrix { border-collapse: collapse; font-size: 12px; }
.cross-matrix th, .cross-matrix td { border: 1px solid #ebeef5; padding: 5px 8px; text-align: center; white-space: nowrap; }
.cross-matrix th { background: #f5f7fa; font-weight: 600; }

.retention-matrix { border-collapse: collapse; font-size: 12px; }
.retention-matrix th, .retention-matrix td { border: 1px solid #ebeef5; padding: 6px 10px; text-align: center; white-space: nowrap; }
.retention-matrix th { background: #f5f7fa; font-weight: 600; }
</style>
