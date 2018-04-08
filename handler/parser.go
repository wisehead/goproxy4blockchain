package handler

/*
1.种子信息：
生产企业：黑龙江省五常市龙洋种子有限公司
注册号： 912301847345944427
统一社会信用代码： 912301847345944427
五优稻4号（稻花香2号）
种子审定编号：黑审稻2009005
品质：经品质分析糙米率84.1%，精米率75.7%，整精米率66.8%，粒长6.6mm,胶稠度67.0%，食味评分88-92。
*/
type QulityInfo struct {
	UnpolishedRiceRate string `json:"unpolishedRicePercentage"`
	PolishedRiceRate   string `json:"polishedRiceRate"`
	HeadRiceRate       string `json:"headRiceRate"`
	GrainLength        string `json:"grainLength"`
	GelConsistency     string `json:"gelConsistency"`
	TasteScore         string `json:"tasteScore"`
}

type SeedInfo struct {
	Company                 string     `json:"company"`
	RegisteredNumber        string     `json:"registeredNumber"`
	UnifiedSocialCreditCode string     `json:"unifiedSocialCreditCode"`
	ProductInfo             string     `json:"productInfo"`
	SeedValidationNumber    string     `json:"seedValidationNumber"`
	Qulity                  QulityInfo `json:"qulity"`
}

/*
2.生物有机肥信息
生产企业：哈尔滨恒丰源农业科技发展有限责任公司
统一社会信用代码:912301030780694118
组织机构代码:078069411
注册号:230103100339348
产品信息：恒丰源生物有机肥
酵素≥8%；有机质≥45%；氨基酸≥12%；腐殖酸≥10%
*/
type BiologicalOrganicFertilizerInfo struct {
	Company                 string                  `json:"company"`
	UnifiedSocialCreditCode string                  `json:"unifiedSocialCreditCode"`
	OrganizationCode        string                  `json:"organizationCode"`
	RegisteredNumber        string                  `json:"registeredNumber"`
	ProductInfo             string                  `json:"productInfo"`
	ChemicalComposition     ChemicalCompositionInfo `json:"chemicalComposition"`
}

type ChemicalCompositionInfo struct {
	Enzyme        string `json:"enzyme"`
	OrganicMatter string `json:"organicMatter"`
	AminoAcid     string `json:"aminoAcid"`
	HumicAcid     string `json:"humicAcid"`
}

/*
3.有机认证信息
基地有机证编号：227OP1600100
加工有机证编号：227OP1600099
*/
type OrganicAuthenticationInfo struct {
	OrganicEvidenceInBaseNum     string `json:"organicEvidenceInBaseNum"`
	ProcessingOrganicSyndromeNum string `json:"processingOrganicSyndromeNum"`
}

/*4.产品信息
大米检验报告单
序号	检验项目	计量单位	标准要求	实测值	单项结论
1	感官要求		应符合标准要求
2	加工精度		应符合标准要求
3	黄粒米	%	≤0.1
4	不完善粒	%	≤0.5
5	杂质总量	%	≤0.10
	糠    粉	%	≤0.02
	矿 物 质	%	0
6	碎米总量	%	≤10.0
	小 碎 米	%	≤0.2
7	水   份	%	≤15.5
8	垩白粒率	%	≤15
9	食味品质	分	≥85
10	直链淀粉	%	15-20
11	胶 稠 度	mm	≥70
12	霉 变 粒	%	≤2.0
*/
type ProductInfomation struct {
	Items []ProductItem `json:"items"`
}

type ProductItem struct {
	ID                   string `json:"id"`
	InspectionProject    string `json:"inspectionProject"`
	MeasurementUnit      string `json:"measurementUnit"`
	StandardRequirements string `json:"standardRequirements"`
	MeasuredValue        string `json:"measuredValue"`
	SingleConclusion     string `json:"singleConclusion"`
}

/*5.土壤检测报告（数据辛苦从图片中提取即可）*/
type SoilCheckReport struct {
	Items []SoilCheckReportItem `json:"items"`
}

type SoilCheckReportItem struct {
	SmapleId                 string `json:"smapleId"`
	SampleInspectionProject  string `json:"sampleInspectionProject"`
	SoilUnit                 string `json:"soilUnit"`
	SoilIndex                string `json:"soilIndex"`
	SoilMeasuredData         string `json:"soilMeasuredData"`
	SampleNumDetectionLimit  string `json:"sampleNumDetectionLimit"`
	LN166872SingleConclusion string `json:"ln166872SingleConclusion"`
	LN166872DetectionBasis   string `json:"ln166872DetectionBasis"`
}
