"""
基金资讯 AI Function 分析
直接调用 Doris 内置 AI Function，使用 qwen_llm resource：
  AI_SUMMARIZE('qwen_llm', content)
  AI_SENTIMENT('qwen_llm', content)
  AI_EXTRACT  ('qwen_llm', content, ARRAY(...))
"""
import json
import re
from datetime import datetime, timedelta
from backend.doris.connect import execute_query, execute_write, execute_many

_LLM = "qwen_llm"

_EXTRACT_LABELS = ["事件类型", "影响板块", "关键政策或技术", "核心公司", "市场影响方向"]


def _parse_extract(ex):
    """兼容 JSON 和 Doris AI_EXTRACT 返回的 key=value 格式
    示例: 事件类型=政策发布,影响板块=半导体板块,核心公司=中芯国际、北方华创
    """
    if not ex:
        return {}
    if isinstance(ex, dict):
        return ex
    ex = str(ex).strip()
    # 尝试 JSON
    if ex.startswith('{'):
        try:
            return json.loads(ex)
        except Exception:
            pass
    # 解析 key=value 格式：以","分隔键值对，"、"分隔多值
    result = {}
    for pair in re.split(r',(?=[\u4e00-\u9fff])', ex):
        if '=' not in pair:
            continue
        key, _, val = pair.partition('=')
        key = key.strip()
        val = val.strip()
        if not val:
            continue
        result[key] = [v.strip() for v in val.split('、') if v.strip()] if '、' in val else val
    return result

# ── 模拟资讯库（25条，覆盖各板块）───────────────────────────────
_RAW_NEWS = [
    {
        "id": "N001", "sector": "半导体",
        "title": "工信部：2025年半导体国产化率目标提升至70%，重点支持EDA与先进封装",
        "source": "财联社",
        "content": "工业和信息化部昨日正式印发《新一代半导体产业高质量发展行动方案（2025-2027）》，明确提出到2025年底，集成电路关键材料、设备及EDA工具的国产化率整体提升至70%以上。方案重点支持先进封装、Chiplet异构集成及第三代半导体材料三大领域，并首次将EDA软件列入专项攻关清单，给予最高3亿元的研发补贴。受政策利好刺激，A股半导体板块早盘全线拉升，中芯国际、北方华创、华海清科等龙头股涨幅均超5%。分析人士指出，本次方案从设备、材料、工具链全链路部署支持，政策力度为近年来最强，将有效推动国内半导体产业链完善。"
    },
    {
        "id": "N002", "sector": "半导体",
        "title": "美国商务部宣布新一轮芯片出口管制，针对14nm以下制程设备",
        "source": "证券时报",
        "content": "美国商务部周三宣布扩大对华半导体设备出口管制范围，新增限制覆盖14纳米及以下制程所需的光刻设备、量测设备及部分化学品，涉及应用材料、泛林半导体、KLA等主要设备供应商。消息公布后，A股半导体板块出现明显回调，上证半导体指数下跌超3%，沪硅产业、华虹半导体跌幅居前。业内专家表示，此次管制进一步压缩国内芯片厂商先进制程升级的时间窗口，短期将对产能扩张计划形成压力。部分机构认为，中长期来看将加速国产替代进程，国内设备商有望受益。"
    },
    {
        "id": "N003", "sector": "半导体",
        "title": "中芯国际Q3业绩超预期：营收同比增长34%，12英寸产能利用率突破92%",
        "source": "上海证券报",
        "content": "中芯国际发布三季度财报，实现营收21.7亿美元，同比增长34.2%，超出市场预期均值约8%。毛利率恢复至18.6%，环比提升2.8个百分点，主要受益于消费电子复苏带动的订单回暖及12英寸产能利用率提升。公司管理层表示，12英寸产能利用率已突破92%，成熟制程供需基本平衡，预计四季度收入环比继续增长4%-6%。汽车电子、工业控制及AI推理芯片成为重点增长驱动力。机构投资者普遍上调目标价，部分机构给出年内最高预期。"
    },
    {
        "id": "N004", "sector": "新能源",
        "title": "11月新能源汽车销量创历史新高：渗透率首破50%，比亚迪单月交付52万辆",
        "source": "中国证券报",
        "content": "乘联会数据显示，11月新能源乘用车零售销量达157万辆，同比增长52%，市场渗透率首次突破50%大关，标志着新能源汽车进入主流消费阶段。比亚迪单月交付52.3万辆，刷新历史纪录，宋、汉、海鸥系列均创月销新高。特斯拉上海工厂交付8.1万辆，环比增长18%。政策层面，多个城市延续以旧换新补贴至年底，部分省份进一步加码补贴额度至1.5万元/辆。受销量数据提振，新能源整车及产业链个股普遍上涨，宁德时代涨幅超4%，三元锂与铁锂材料股集体走强。"
    },
    {
        "id": "N005", "sector": "新能源",
        "title": "光伏装机量首超煤电成为第一大电源，隆基绿能宣布BC技术量产效率突破27%",
        "source": "财联社",
        "content": "国家能源局最新数据显示，截至三季度末，全国光伏累计装机容量达8.2亿千瓦，首次超越煤电成为我国第一大电源，具有重要历史意义。与此同时，隆基绿能宣布其新一代HPBC 2.0电池量产平均转换效率突破27.1%，刷新行业量产效率纪录，较上代产品提升约1.2个百分点。公司表示将在明年一季度完成全面切换，届时成本较PERC下降10%以上。消息带动光伏板块普涨，天合光能、晶科能源、TCL中环分别涨超3%。分析人士认为，BC技术路线确立领先地位，有望推动行业格局重塑。"
    },
    {
        "id": "N006", "sector": "消费",
        "title": "双十一总成交额突破1.4万亿元，白酒电商渗透率同比提升8个百分点",
        "source": "证券时报",
        "content": "各主要电商平台公布2024年双十一最终数据，合计总成交额突破1.4万亿元，同比增长约12%。白酒品类表现亮眼，电商渗透率从去年同期的14%提升至22%，贵州茅台、五粮液、洋河股份均创双十一销售记录。家电、美妆、服装等传统品类增速在8%-15%区间，符合市场预期。值得关注的是，直播电商占比首次超过传统货架电商，抖音平台GMV同比增长76%。消费板块个股午后整体走强，贵州茅台涨1.8%，海天味业、格力电器、伊利股份跟涨。机构维持消费板块超配评级，建议关注高端消费复苏机会。"
    },
    {
        "id": "N007", "sector": "消费",
        "title": "10月社零总额同比增长4.8%，餐饮消费连续6个月跑赢商品零售",
        "source": "上海证券报",
        "content": "国家统计局公布10月社会消费品零售总额数据，同比增长4.8%，环比9月提升0.6个百分点，好于市场预期的4.3%。分品类看，餐饮收入同比增长7.2%，连续6个月增速高于商品零售，显示服务消费持续发力。商品零售中，汽车类增长9.1%，家电类增长12.5%，受以旧换新政策支持明显。黄金珠宝类受高金价抑制，增速仅为2.1%。分析师认为，消费数据整体向好，但居民消费信心仍需持续观察，建议关注政策持续支持下的消费复苏板块投资机会，重点推荐大众消费和服务业龙头标的。"
    },
    {
        "id": "N008", "sector": "医药",
        "title": "国家药监局批准重磅国产PD-1新适应症，信达生物股价单日暴涨18%",
        "source": "财联社",
        "content": "国家药品监督管理局昨日正式批准信达生物PD-1抑制剂信迪利单抗新增非小细胞肺癌一线治疗适应症，成为国内第四款获批该适应症的PD-1产品，但在价格和医保谈判方面具备显著优势。受此消息影响，信达生物股价单日涨幅达18.3%，创近两年最大单日涨幅，带动医药板块整体上扬。君实生物、百济神州等PD-1相关标的联动上涨3%-8%。分析人士指出，国产PD-1在新适应症竞争中格局持续清晰，头部企业护城河加深，建议关注具备差异化适应症管线的创新药企。"
    },
    {
        "id": "N009", "sector": "医药",
        "title": "CXO行业景气度持续下行：药明康德三季报营收下滑12%，欧美客户订单萎缩",
        "source": "中国证券报",
        "content": "药明康德发布三季度报告，实现营收97.4亿元，同比下滑12.3%，低于市场预期；归母净利润24.6亿元，同比下降18.1%。公司表示，美国及欧洲生物医药客户受融资环境收紧影响，研发外包预算明显削减，新签订单同比减少约25%。公司已启动成本压缩计划，拟裁减非核心业务人员约3000人。受业绩拖累，CXO板块全线下跌，泰格医药、康龙化成、昭衍新药跌幅均超5%，板块估值面临进一步压缩压力。部分机构下调行业评级至中性，建议谨慎配置直至海外创新药融资环境明显改善。"
    },
    {
        "id": "N010", "sector": "金融",
        "title": "央行宣布降准0.5个百分点，释放长期流动性约1万亿元",
        "source": "上海证券报",
        "content": "中国人民银行宣布，自本月20日起下调金融机构存款准备金率0.5个百分点（不含已执行5%存款准备金率的金融机构），本次降准预计释放长期流动性约1万亿元。央行表示，此次降准旨在保持银行体系流动性合理充裕，支持实体经济高质量发展。消息公布后，A股市场全线上涨，金融板块领涨，招商银行、平安银行涨幅超3%，中信证券、华泰证券等券商股跟涨。债市同步走强，10年期国债收益率下行约4个基点至2.12%。分析师认为，本次降准信号意义强烈，后续仍有降息预期，利好整体金融市场流动性环境。"
    },
    {
        "id": "N011", "sector": "军工",
        "title": "国防预算增长7.2%，歼-35A正式亮相航展，军工板块进入景气上行周期",
        "source": "财联社",
        "content": "财政部公布最新国防预算数据，全年国防支出同比增长7.2%，增速较去年提升0.5个百分点，连续29年保持增长。与此同时，歼-35A隐身舰载战斗机在珠海航展正式公开亮相，展示了多项先进航电与武器系统，引发广泛关注。受双重利好刺激，军工板块强势上涨，中航沈飞涨停，航发动力、洪都航空分别上涨8.6%、7.2%。分析人士指出，国防预算持续增长叠加装备更新换代加速，军工板块将进入景气上行周期，建议重点关注歼击机产业链及电子对抗领域核心标的。"
    },
    {
        "id": "N012", "sector": "宏观",
        "title": "10月CPI同比上涨0.3%，PPI连续25个月负增长通缩压力仍存",
        "source": "中国证券报",
        "content": "国家统计局公布10月物价数据：CPI同比上涨0.3%，低于预期的0.4%，环比持平；PPI同比下降2.9%，降幅较上月扩大0.1个百分点，已连续25个月呈现负增长。CPI数据中，食品项价格上涨0.9%，受猪肉价格影响；非食品项价格持平。服务价格同比涨0.6%，核心CPI维持在0.2%低位。经济学家认为，通缩风险仍是当前主要挑战，内需修复不足、工业品价格下行压力尚未消退，预计政策端将在货币宽松基础上进一步加码财政刺激，以提振需求侧。债券市场对通缩数据反应积极，国债收益率小幅下行。"
    },
    {
        "id": "N013", "sector": "宏观",
        "title": "三季度GDP增速4.6%，全年5%目标基本确定，科技与消费双轮驱动",
        "source": "证券时报",
        "content": "国家统计局发布三季度GDP数据，同比增长4.6%，好于市场预期的4.4%，前三季度累计增速为4.8%。分析人士指出，在外贸对GDP贡献边际减弱背景下，科技制造与大众消费成为支撑增长的双重引擎。其中，高技术制造业增加值同比增长10.2%，消费对GDP贡献率升至49.5%。全年完成5%左右增速目标基本无悬念，部分机构小幅上调全年预测至5.1%。A股市场整体平稳，权重指数小幅收涨，外资净流入规模扩大，对中国经济基本面信心有所修复。建议投资者关注受益于高端制造和内需扩张的成长板块。"
    },
    {
        "id": "N014", "sector": "新能源",
        "title": "储能市场爆发：前三季度新增装机同比增长210%，宁德时代拿下全球最大项目",
        "source": "财联社",
        "content": "中国化学与物理电源行业协会数据显示，前三季度国内新型储能新增装机达38.6GWh，同比增长210%，其中大型地面电站储能占比提升至68%。宁德时代宣布中标沙特阿美集团全球最大储能项目，装机规模达12GWh，合同金额超60亿元，彰显其全球储能龙头地位。锂电池储能配套政策持续完善，多省出台强制储能配储比例要求，政策驱动与市场需求共振。板块方面，储能概念股整体上涨，阳光电源涨6.7%，宁德时代涨3.2%，汇川技术、固德威等跟涨。分析师维持板块增持评级，重点推荐具备全球竞争力的储能系统集成商。"
    },
    {
        "id": "N015", "sector": "半导体",
        "title": "华为Mate 70发布：搭载麒麟9100芯片，5G性能接近旗舰水准引发市场热议",
        "source": "上海证券报",
        "content": "华为正式发布Mate 70系列旗舰手机，搭载最新麒麟9100处理器，采用中芯国际7nm+工艺制造，实测5G峰值下载速率达1.2Gbps，已接近高通骁龙8 Gen3的80%性能水平。Mate 70发布当天，预约量突破500万台，创华为史上最高预约纪录。受此消息影响，A股消费电子产业链全线上涨，欧菲光涨停，瑞声科技、蓝思科技、立讯精密分别上涨6%-9%。与此同时，国产半导体供应链配套比例进一步提升至约70%，对海思、华润微、韦尔股份等芯片设计及代工企业形成直接利好，机构普遍上调目标价。"
    },
    {
        "id": "N016", "sector": "金融",
        "title": "证监会出台公募基金费率改革第三阶段方案，管理费上限降至0.8%",
        "source": "中国证券报",
        "content": "证监会发布公募基金费率改革第三阶段实施方案，核心内容包括：主动权益类基金管理费率上限由1.2%进一步下调至0.8%，托管费率上限降至0.15%；同时建立与基金业绩挂钩的浮动费率机制，超额收益部分提取20%绩效分成。新规将于明年一季度正式生效，存量基金给予6个月过渡期。消息公布后，基金公司股及相关概念股普遍下跌，天天基金、蚂蚁集团港股跌约3%。但部分机构认为，长期看费率改革有助于行业健康发展，可能倒逼头部基金公司向主动管理能力分化，利好投资者群体。"
    },
    {
        "id": "N017", "sector": "化工",
        "title": "万华化学MDI价格连续8周上涨，景气周期确认，机构密集调研",
        "source": "财联社",
        "content": "MDI（异氰酸酯）价格近期持续走强，纯MDI报价突破2.8万元/吨，聚合MDI报价达1.85万元/吨，均创近18个月新高，且连续8周保持涨势。业内人士表示，供给侧欧洲工厂计划外停工叠加中国下游家电、建筑保温需求回暖，共同推动本轮价格上行。万华化学作为全球MDI龙头，有望充分受益本轮景气周期，多家机构在其年内高位时密集调研，纷纷上调盈利预测。化工板块今日整体表现强势，万华化学涨4.3%，华鲁恒升、卫星化学、合盛硅业集体跟涨。机构预计万华三季报净利润同比增长超35%。"
    },
    {
        "id": "N018", "sector": "传媒",
        "title": "国产游戏出海创新高：前三季度海外收入突破180亿美元，腾讯网易占据半壁江山",
        "source": "证券时报",
        "content": "游戏工委联合伽马数据发布报告，前三季度中国自主研发游戏海外市场实际销售收入达180.4亿美元，同比增长15.3%，超过去年全年总量，创历史同期最高。腾讯、网易合计市场份额达51%，但头部效应趋势减弱，中小厂商占比提升明显。东南亚、中东及欧美市场成为增量主战场，SLG策略类和二次元RPG是出海最受欢迎品类。传媒及游戏板块在数据公布后走强，吉比特涨5.1%，三七互娱、巨人网络集体上扬。分析师看好出海势头持续，建议重点关注具备海外运营能力和IP矩阵的头部游戏公司。"
    },
    {
        "id": "N019", "sector": "医药",
        "title": "GLP-1减肥药国内获批加速：诺和诺德司美格鲁肽注射液正式上市，国产替代备受期待",
        "source": "财联社",
        "content": "国家药监局正式批准诺和诺德司美格鲁肽注射液（商品名：诺和盈）肥胖适应症在华上市，成为国内首款获批减重适应症的GLP-1受体激动剂。上市首日各大平台预约量迅速爆满，肥胖症专科门诊挂号难度显著上升。与此同时，国内多家药企GLP-1管线进展备受关注：华东医药、信达生物、恒瑞医药均有在研产品进入III期临床，最快有望在2025年下半年提交上市申请。受GLP-1赛道整体火热影响，原料药供应商诺泰生物、九源基因涨停，相关产业链标的集体拉升。机构认为这是近年来国内最受期待的新药市场之一。"
    },
    {
        "id": "N020", "sector": "新能源",
        "title": "欧盟正式对中国电动汽车加征关税：比亚迪17%、吉利19%、上汽35%",
        "source": "中国证券报",
        "content": "欧盟委员会宣布，对中国进口电动汽车征收最终反补贴税，其中比亚迪税率为17.0%、吉利为18.8%、上汽最高达35.3%，其他中国车企适用20.7%的平均税率，自即日起正式生效，有效期五年。消息公布后，新能源整车板块遭遇重挫，比亚迪下跌3.8%，吉利汽车港股跌超6%。但也有分析人士指出，欧盟市场仅占中国车企出口总量的约15%，东南亚、中东、拉美市场的快速增长有望有效对冲欧盟关税冲击。政策层面，商务部表示将继续通过对话协商解决分歧，同时已启动欧盟白兰地进口调查作为反制措施。"
    },
    {
        "id": "N021", "sector": "金融",
        "title": "沪深两市日成交额突破3万亿元历史天量，北向资金单日净买入超260亿",
        "source": "上海证券报",
        "content": "沪深两市昨日合计成交额达3.08万亿元，刷新A股历史记录，超越2015年牛市高峰。市场情绪空前高涨，主要宽基指数集体大涨，上证指数涨3.1%，创业板指涨4.8%，北向资金全天净买入261亿元，同样创近年新高。成交量放大主要受三重因素驱动：降准降息政策落地、上市公司回购增持规模超预期以及险资入市节奏明显加快。市场分析人士认为，日成交量突破3万亿标志着A股进入新一轮上涨周期初期，建议重点布局高股息蓝筹与政策受益的科技成长，保持进攻性配置策略。"
    },
    {
        "id": "N022", "sector": "半导体",
        "title": "国产EDA软件突破：华大九天EDA全流程工具链通过28nm工艺验证",
        "source": "财联社",
        "content": "华大九天正式宣布，其自主研发的全流程EDA工具链完成28纳米工艺节点全流程流片验证，覆盖从前端逻辑综合到后端物理实现的完整设计闭环，成为国内首家实现此目标的本土EDA企业。EDA被称为芯片设计的灵魂工具，长期被Synopsys、Cadence、Mentor三巨头垄断，本次突破具有重要战略意义。消息公布后，华大九天股价涨停，并带动芯原股份、概伦电子等EDA相关标的集体大涨。行业人士表示，28nm全流程打通意味着国内EDA产业完成重要里程碑，下一步向14nm节点突破仍需2-3年攻关周期。"
    },
    {
        "id": "N023", "sector": "消费",
        "title": "春节消费数据出炉：全国旅游收入8758亿创纪录，免税消费同比增长31%",
        "source": "证券时报",
        "content": "文旅部发布春节黄金周数据：全国国内旅游出游人次达9.01亿，同比增长19.2%；实现国内旅游收入8758亿元，同比增长23.1%，均创历史新高。三亚、张家界、九寨沟等热门景区出现门票即开即售的火爆场景。海南离岛免税销售额同比增长31%，黄金珠宝、化妆品、名表为前三大品类。餐饮市场同样亮眼，重庆火锅、北京烤鸭、上海本帮菜预订供不应求。大消费板块在假期数据公布后持续走强，中国国旅、宋城演艺、同程旅行均创近期新高。分析师认为，消费复苏趋势确立，出行服务与可选消费将是下半年布局重点。"
    },
    {
        "id": "N024", "sector": "军工",
        "title": "歼-20B换装国产涡扇-15发动机，标志中国战机发动机实现完全自主可控",
        "source": "中国证券报",
        "content": "据多方信源证实，歼-20B第五代隐身战斗机已完成换装国产涡扇-15大推力发动机，该发动机推力达18吨级，达到世界一流水准，彻底解决了歼-20长期依赖过渡型发动机的历史问题，标志着中国战机发动机实现完全自主可控。军工业内人士表示，航空发动机是军工领域最后一块难啃的骨头，此次突破意味着歼-20战斗力出现质的提升。军工板块应声上涨，航发动力涨停，中航沈飞、中航机电涨幅超8%，带动整个国防板块走强。机构认为军机换发带来的批产需求将持续释放，航发产业链迎来黄金期。"
    },
    {
        "id": "N025", "sector": "宏观",
        "title": "美联储宣布降息25BP：全球流动性拐点确认，新兴市场迎来资金回流窗口",
        "source": "财联社",
        "content": "美联储在9月议息会议上宣布将联邦基金利率目标区间下调25个基点至4.50%-4.75%，为2020年以来首次降息，标志着本轮加息周期正式结束。鲍威尔在发布会表示，通胀已接近2%目标，就业市场降温信号明确，未来将视数据审慎调整。受降息消息影响，美股三大指数齐创历史新高，美元指数下跌0.8%。人民币兑美元汇率快速升值至7.05附近，北向资金加速涌入。分析师认为，全球流动性拐点确认，新兴市场尤其是A股和港股将受益于资金回流，建议超配权益资产，关注高弹性的科技成长和红利价值双主线。"
    },
]


class NewsService:

    # ── 建表（仅首次，IF NOT EXISTS，不清数据）──────────────
    async def init_table(self):
        await execute_write("""
            CREATE TABLE IF NOT EXISTS news_article (
                article_id      VARCHAR(10)   NOT NULL,
                publish_ts      DATETIME      NOT NULL,
                title           VARCHAR(300),
                content         TEXT,
                source          VARCHAR(50),
                sector_tag      VARCHAR(30),
                ai_summary      VARCHAR(500),
                summarized      TINYINT       DEFAULT 0,
                ai_sentiment    VARCHAR(20),
                sentiment_score INT,
                sentiment_done  TINYINT       DEFAULT 0,
                ai_extract      VARCHAR(2000),
                extracted       TINYINT       DEFAULT 0,
                ai_method       VARCHAR(20)   DEFAULT 'PENDING'
            ) UNIQUE KEY(article_id, publish_ts)
            DISTRIBUTED BY HASH(article_id) BUCKETS 4
            PROPERTIES("replication_num"="1")
        """)
        return {"msg": "表已就绪（IF NOT EXISTS，数据保留）"}

    # ── 导入模拟资讯（追加写入，不清除历史）────────────────
    async def import_news(self):
        base_ts = datetime.now() - timedelta(hours=len(_RAW_NEWS))
        rows = []
        for i, n in enumerate(_RAW_NEWS):
            ts = (base_ts + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")
            rows.append((
                n["id"], ts, n["title"], n["content"],
                n["source"], n["sector"],
                None, 0, None, None, 0, None, 0, "PENDING"
            ))
        await execute_many(
            "INSERT INTO news_article VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            rows
        )
        return {"msg": f"成功写入 {len(rows)} 条资讯到 Doris", "count": len(rows)}

    # ── 获取文章列表 ────────────────────────────────────────
    async def get_list(self, sector: str = None, sentiment: str = None, keyword: str = None):
        where = ["1=1"]
        if sector:    where.append(f"sector_tag='{sector}'")
        if sentiment: where.append(f"ai_sentiment='{sentiment}'")
        if keyword:   where.append(f"(title LIKE '%{keyword}%' OR content LIKE '%{keyword}%')")
        sql = f"""
            SELECT article_id, publish_ts, title, source, sector_tag,
                   ai_summary, ai_sentiment, sentiment_score,
                   ai_extract, summarized, sentiment_done, extracted, ai_method
            FROM news_article
            WHERE {' AND '.join(where)}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY publish_ts DESC) = 1
            ORDER BY publish_ts DESC
        """
        return await execute_query(sql) or []

    async def get_detail(self, article_id: str):
        rows = await execute_query(f"""
            SELECT * FROM news_article
            WHERE article_id = '{article_id}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY publish_ts DESC) = 1
        """)
        return rows[0] if rows else {}

    # ── AI_SUMMARIZE ────────────────────────────────────────
    async def run_summarize(self, article_ids: list = None):
        where = self._build_where("summarized", 0, article_ids)
        sql_display = f"""-- Doris AI_SUMMARIZE：对金融资讯正文进行高度概括
-- Resource: {_LLM}  (qwen-plus, QWEN provider)
UPDATE news_article
SET ai_summary = AI_SUMMARIZE('{_LLM}', content),
    summarized  = 1,
    ai_method   = 'DORIS_AI_FUNCTION'
WHERE {where}"""

        rc = await execute_write(f"""
            UPDATE news_article
            SET ai_summary = AI_SUMMARIZE('{_LLM}', content),
                summarized  = 1,
                ai_method   = 'DORIS_AI_FUNCTION'
            WHERE {where}
        """)
        return {
            "msg": f"AI_SUMMARIZE 完成，处理 {rc} 篇",
            "processed": rc,
            "sql": sql_display,
        }

    # ── AI_SENTIMENT ────────────────────────────────────────
    async def run_sentiment(self, article_ids: list = None):
        where = self._build_where("sentiment_done", 0, article_ids)
        sql_display = f"""-- Doris AI_SENTIMENT：分析情感倾向
-- 返回值: positive | negative | neutral | mixed
-- Resource: {_LLM}  (qwen-plus, QWEN provider)
UPDATE news_article
SET ai_sentiment  = AI_SENTIMENT('{_LLM}', content),
    sentiment_done = 1,
    ai_method      = 'DORIS_AI_FUNCTION'
WHERE {where}"""

        # Step 1: 情感分类
        rc = await execute_write(f"""
            UPDATE news_article
            SET ai_sentiment  = AI_SENTIMENT('{_LLM}', content),
                sentiment_done = 1,
                ai_method      = 'DORIS_AI_FUNCTION'
            WHERE {where}
        """)
        # Step 2: 根据情感类型映射评分
        score_where = self._build_where("sentiment_done", 1, article_ids)
        await execute_write(f"""
            UPDATE news_article
            SET sentiment_score = CASE ai_sentiment
                WHEN 'positive' THEN  70
                WHEN 'negative' THEN -70
                WHEN 'mixed'    THEN  15
                ELSE 0
            END
            WHERE {score_where}
        """)
        return {
            "msg": f"AI_SENTIMENT 完成，处理 {rc} 篇",
            "processed": rc,
            "sql": sql_display,
        }

    # ── AI_EXTRACT ──────────────────────────────────────────
    async def run_extract(self, article_ids: list = None):
        where = self._build_where("extracted", 0, article_ids)
        labels_sql = "ARRAY(" + ",".join(f"'{l}'" for l in _EXTRACT_LABELS) + ")"
        sql_display = f"""-- Doris AI_EXTRACT：按标签列表提取结构化信息（返回 JSON）
-- Resource: {_LLM}  (qwen-plus, QWEN provider)
UPDATE news_article
SET ai_extract = AI_EXTRACT(
        '{_LLM}', content,
        {labels_sql}
    ),
    extracted = 1,
    ai_method = 'DORIS_AI_FUNCTION'
WHERE {where}"""

        rc = await execute_write(f"""
            UPDATE news_article
            SET ai_extract = AI_EXTRACT(
                    '{_LLM}', content,
                    {labels_sql}
                ),
                extracted = 1,
                ai_method = 'DORIS_AI_FUNCTION'
            WHERE {where}
        """)
        return {
            "msg": f"AI_EXTRACT 完成，处理 {rc} 篇",
            "processed": rc,
            "sql": sql_display,
        }

    # ── 统计进度 ────────────────────────────────────────────
    async def get_stats(self):
        rows = await execute_query("""
            SELECT
                COUNT(DISTINCT article_id) AS total,
                SUM(summarized)            AS summarized,
                SUM(sentiment_done)        AS sentiment_done,
                SUM(extracted)             AS extracted
            FROM (
                SELECT article_id, summarized, sentiment_done, extracted
                FROM news_article
                QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY publish_ts DESC) = 1
            ) t
        """) or []
        return rows[0] if rows else {"total": 0, "summarized": 0, "sentiment_done": 0, "extracted": 0}

    # ── 标签分析 ────────────────────────────────────────────
    async def get_tag_analysis(self):
        rows = await execute_query("""
            SELECT article_id, sector_tag, ai_sentiment, sentiment_score, ai_extract
            FROM news_article
            WHERE extracted = 1
            QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY publish_ts DESC) = 1
        """) or []

        sentiment_dist = {}
        sector_sentiment = {}
        tag_freq = {}

        for r in rows:
            s = r.get("ai_sentiment") or "neutral"
            sentiment_dist[s] = sentiment_dist.get(s, 0) + 1
            sec = r.get("sector_tag") or "其他"
            if sec not in sector_sentiment:
                sector_sentiment[sec] = {"positive": 0, "negative": 0, "neutral": 0, "mixed": 0, "total": 0}
            sector_sentiment[sec][s] = sector_sentiment[sec].get(s, 0) + 1
            sector_sentiment[sec]["total"] += 1

            ex = r.get("ai_extract")
            if ex:
                data = _parse_extract(ex)
                for label, val in data.items():
                    if isinstance(val, list):
                        for v in val:
                            if v:
                                tag_freq[f"{label}:{v}"] = tag_freq.get(f"{label}:{v}", 0) + 1
                    elif val:
                        tag_freq[f"{label}:{val}"] = tag_freq.get(f"{label}:{val}", 0) + 1

        top_tags = sorted(tag_freq.items(), key=lambda x: -x[1])[:30]
        return {
            "sentiment_dist": sentiment_dist,
            "sector_sentiment": sector_sentiment,
            "top_tags": [{"tag": k, "freq": v} for k, v in top_tags],
            "total": len(rows),
        }

    # ── 板块情感指标聚合 ────────────────────────────────────
    async def get_sector_metrics(self):
        rows = await execute_query("""
            SELECT
                sector_tag,
                COUNT(DISTINCT article_id)                                AS article_count,
                ROUND(AVG(sentiment_score), 1)                            AS avg_score,
                SUM(CASE WHEN ai_sentiment='positive' THEN 1 ELSE 0 END)  AS positive_cnt,
                SUM(CASE WHEN ai_sentiment='negative' THEN 1 ELSE 0 END)  AS negative_cnt,
                SUM(CASE WHEN ai_sentiment='neutral'  THEN 1 ELSE 0 END)  AS neutral_cnt,
                SUM(CASE WHEN ai_sentiment='mixed'    THEN 1 ELSE 0 END)  AS mixed_cnt,
                MAX(publish_ts)                                           AS latest_ts
            FROM (
                SELECT article_id, sector_tag, ai_sentiment, sentiment_score, publish_ts
                FROM news_article
                WHERE sentiment_done = 1
                QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY publish_ts DESC) = 1
            ) t
            GROUP BY sector_tag
            ORDER BY avg_score DESC
        """) or []
        for r in rows:
            total = r.get("article_count") or 1
            r["positive_ratio"] = round((r.get("positive_cnt") or 0) / total * 100, 1)
            r["negative_ratio"] = round((r.get("negative_cnt") or 0) / total * 100, 1)
        return rows

    # ── 投资信号（板块情感→信号） ────────────────────────────
    async def get_signals(self):
        rows = await execute_query("""
            SELECT
                sector_tag,
                ROUND(AVG(sentiment_score), 1)                           AS avg_score,
                COUNT(DISTINCT article_id)                                AS cnt,
                SUM(CASE WHEN ai_sentiment='positive' THEN 1 ELSE 0 END)  AS pos,
                SUM(CASE WHEN ai_sentiment='negative' THEN 1 ELSE 0 END)  AS neg,
                SUM(CASE WHEN ai_sentiment='neutral'  THEN 1 ELSE 0 END)  AS neu,
                SUM(CASE WHEN ai_sentiment='mixed'    THEN 1 ELSE 0 END)  AS mix
            FROM (
                SELECT article_id, sector_tag, ai_sentiment, sentiment_score
                FROM news_article
                WHERE sentiment_done = 1
                QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY publish_ts DESC) = 1
            ) t
            GROUP BY sector_tag
        """) or []
        signals = []
        for r in rows:
            score = float(r.get("avg_score") or 0)
            cnt   = r.get("cnt") or 0
            if score >= 35:
                sig = "bullish"
            elif score <= -20:
                sig = "bearish"
            else:
                sig = "neutral"
            confidence = min(int(abs(score) / 100 * 75 + 25), 95) if sig != "neutral" else 50
            signals.append({
                "sector":        r["sector_tag"],
                "signal":        sig,
                "avg_score":     round(score, 1),
                "confidence":    confidence,
                "article_count": cnt,
                "positive":      r.get("pos") or 0,
                "negative":      r.get("neg") or 0,
                "neutral":       r.get("neu") or 0,
                "mixed":         r.get("mix") or 0,
            })
        signals.sort(key=lambda x: -x["avg_score"])
        return signals

    # ── 热点公司（从 AI_EXTRACT 核心公司字段聚合） ──────────
    async def get_hot_companies(self):
        rows = await execute_query("""
            SELECT article_id, sector_tag, ai_extract
            FROM news_article
            WHERE extracted = 1
            QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY publish_ts DESC) = 1
        """) or []
        company_map: dict = {}
        for r in rows:
            ex = r.get("ai_extract")
            if not ex:
                continue
            try:
                data = _parse_extract(ex)
                companies = data.get("核心公司", [])
                if isinstance(companies, str):
                    companies = [c.strip() for c in companies.split("、") if c.strip()]
                sector = r.get("sector_tag", "其他")
                for c in companies:
                    if c and len(c) > 1:
                        if c not in company_map:
                            company_map[c] = {"company": c, "count": 0, "sectors": set()}
                        company_map[c]["count"] += 1
                        company_map[c]["sectors"].add(sector)
            except Exception:
                pass
        result = [
            {"company": v["company"], "count": v["count"], "sectors": list(v["sectors"])}
            for v in company_map.values()
        ]
        result.sort(key=lambda x: -x["count"])
        return result[:20]

    # ── 内部工具 ────────────────────────────────────────────
    def _build_where(self, flag: str, val: int, ids: list = None) -> str:
        parts = [f"{flag} = {val}"]
        if ids:
            id_str = ",".join(f"'{i}'" for i in ids)
            parts.append(f"article_id IN ({id_str})")
        return " AND ".join(parts)
