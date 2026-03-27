"""
诊断报告生成模块

功能需求: L-07 诊断报告 - 生成PDF/HTML报告
作者: ML Team
"""

import json
import base64
from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from loguru import logger


@dataclass
class DiagnosisReport:
    """诊断报告数据结构"""
    report_id: str
    diagnosis_id: str
    created_at: datetime
    device_name: str
    symptoms: str
    root_cause: str
    confidence: float
    possible_causes: List[str]
    suggested_actions: List[str]
    spare_parts: List[str]
    references: List[Dict]
    similar_cases: List[Dict]
    trend_charts: List[Dict]  # 趋势图表数据
    operator: Optional[str] = None
    notes: Optional[str] = None


class ReportGenerator:
    """
    诊断报告生成器
    
    支持格式:
    - PDF (使用ReportLab或WeasyPrint)
    - HTML (使用Jinja2模板)
    - Markdown
    - JSON
    """
    
    def __init__(self, output_dir: str = "data/reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"报告生成器已初始化: {output_dir}")
    
    def generate_pdf(self, report: DiagnosisReport) -> str:
        """
        生成PDF报告
        
        使用纯Python实现，不依赖外部工具
        """
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import A4
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import cm
            from reportlab.pdfbase import pdfmetrics
            from reportlab.pdfbase.ttfonts import TTFont
        except ImportError:
            logger.error("缺少reportlab依赖，请安装: pip install reportlab")
            return self.generate_html(report)
        
        # 注册中文字体 (使用系统字体或默认字体)
        try:
            pdfmetrics.registerFont(TTFont('SimSun', '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc'))
            chinese_font = 'SimSun'
        except:
            chinese_font = 'Helvetica'
        
        # 创建PDF文档
        filename = f"DIAGNOSIS_{report.report_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        filepath = self.output_dir / filename
        
        doc = SimpleDocTemplate(
            str(filepath),
            pagesize=A4,
            rightMargin=2*cm,
            leftMargin=2*cm,
            topMargin=2*cm,
            bottomMargin=2*cm
        )
        
        # 样式
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontName=chinese_font,
            fontSize=20,
            spaceAfter=30,
            alignment=1  # 居中
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontName=chinese_font,
            fontSize=14,
            spaceAfter=12
        )
        
        normal_style = ParagraphStyle(
            'CustomNormal',
            parent=styles['Normal'],
            fontName=chinese_font,
            fontSize=10,
            spaceAfter=6
        )
        
        # 构建内容
        story = []
        
        # 标题
        story.append(Paragraph("智能诊断报告", title_style))
        story.append(Spacer(1, 20))
        
        # 基本信息
        story.append(Paragraph("一、基本信息", heading_style))
        basic_info = [
            ['报告编号', report.report_id],
            ['诊断编号', report.diagnosis_id],
            ['生成时间', report.created_at.strftime('%Y-%m-%d %H:%M:%S')],
            ['设备名称', report.device_name],
            ['操作人员', report.operator or '系统自动'],
        ]
        
        basic_table = Table(basic_info, colWidths=[4*cm, 10*cm])
        basic_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), chinese_font),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        story.append(basic_table)
        story.append(Spacer(1, 20))
        
        # 症状描述
        story.append(Paragraph("二、故障症状", heading_style))
        story.append(Paragraph(report.symptoms, normal_style))
        story.append(Spacer(1, 12))
        
        # 根因分析
        story.append(Paragraph("三、根因分析", heading_style))
        story.append(Paragraph(f"<b>根本原因:</b> {report.root_cause}", normal_style))
        story.append(Paragraph(f"<b>置信度:</b> {report.confidence*100:.1f}%", normal_style))
        story.append(Spacer(1, 12))
        
        # 可能原因列表
        story.append(Paragraph("可能原因:", normal_style))
        for i, cause in enumerate(report.possible_causes, 1):
            story.append(Paragraph(f"  {i}. {cause}", normal_style))
        story.append(Spacer(1, 12))
        
        # 建议措施
        story.append(Paragraph("四、建议措施", heading_style))
        for i, action in enumerate(report.suggested_actions, 1):
            story.append(Paragraph(f"  {i}. {action}", normal_style))
        story.append(Spacer(1, 12))
        
        # 备件清单
        if report.spare_parts:
            story.append(Paragraph("五、推荐备件", heading_style))
            for part in report.spare_parts:
                story.append(Paragraph(f"  • {part}", normal_style))
            story.append(Spacer(1, 12))
        
        # 参考资料
        if report.references:
            story.append(Paragraph("六、参考资料", heading_style))
            for ref in report.references:
                title = ref.get('title', 'Unknown')
                story.append(Paragraph(f"  • {title}", normal_style))
        
        # 页脚
        story.append(Spacer(1, 30))
        story.append(Paragraph(
            "— 本报告由 Jamin Industrial Agent 智能诊断系统生成 —",
            ParagraphStyle('Footer', parent=normal_style, alignment=1, textColor=colors.grey)
        ))
        
        # 生成PDF
        doc.build(story)
        
        logger.info(f"PDF报告已生成: {filepath}")
        return str(filepath)
    
    def generate_html(self, report: DiagnosisReport) -> str:
        """生成HTML报告"""
        filename = f"DIAGNOSIS_{report.report_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        filepath = self.output_dir / filename
        
        # 构建HTML内容
        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>智能诊断报告 - {report.device_name}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .container {{
            background: white;
            padding: 40px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #1890ff;
            text-align: center;
            border-bottom: 3px solid #1890ff;
            padding-bottom: 15px;
        }}
        h2 {{
            color: #333;
            margin-top: 30px;
            padding-left: 10px;
            border-left: 4px solid #1890ff;
        }}
        .info-grid {{
            display: grid;
            grid-template-columns: 120px 1fr;
            gap: 10px;
            margin: 20px 0;
        }}
        .info-label {{
            font-weight: bold;
            color: #666;
        }}
        .confidence-badge {{
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: bold;
            color: white;
            background: {'#52c41a' if report.confidence > 0.8 else '#faad14' if report.confidence > 0.6 else '#f5222d'};
        }}
        .list-item {{
            padding: 8px 0;
            border-bottom: 1px solid #f0f0f0;
        }}
        .footer {{
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #e8e8e8;
            color: #999;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🦞 智能诊断报告</h1>
        
        <h2>一、基本信息</h2>
        <div class="info-grid">
            <span class="info-label">报告编号:</span>
            <span>{report.report_id}</span>
            <span class="info-label">诊断编号:</span>
            <span>{report.diagnosis_id}</span>
            <span class="info-label">生成时间:</span>
            <span>{report.created_at.strftime('%Y-%m-%d %H:%M:%S')}</span>
            <span class="info-label">设备名称:</span>
            <span>{report.device_name}</span>
            <span class="info-label">操作人员:</span>
            <span>{report.operator or '系统自动'}</span>
        </div>
        
        <h2>二、故障症状</h2>
        <p>{report.symptoms}</p>
        
        <h2>三、根因分析</h2>
        <p><strong>根本原因:</strong> {report.root_cause}</p>
        <p><strong>置信度:</strong> <span class="confidence-badge">{report.confidence*100:.1f}%</span></p>
        
        <h3>可能原因:</h3>
        <ol>
            {''.join(f'<li class="list-item">{cause}</li>' for cause in report.possible_causes)}
        </ol>
        
        <h2>四、建议措施</h2>
        <ol>
            {''.join(f'<li class="list-item">{action}</li>' for action in report.suggested_actions)}
        </ol>
        
        {'<h2>五、推荐备件</h2><ul>' + ''.join(f'<li class="list-item">{part}</li>' for part in report.spare_parts) + '</ul>' if report.spare_parts else ''}
        
        {'<h2>六、参考资料</h2><ul>' + ''.join(f'<li class="list-item">{ref.get("title", "")}</li>' for ref in report.references) + '</ul>' if report.references else ''}
        
        <div class="footer">
            <p>本报告由 Jamin Industrial Agent 智能诊断系统生成</p>
            <p>© 2024 Jamin Industrial Agent</p>
        </div>
    </div>
</body>
</html>
"""
        
        # 写入文件
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML报告已生成: {filepath}")
        return str(filepath)
    
    def generate_markdown(self, report: DiagnosisReport) -> str:
        """生成Markdown报告"""
        filename = f"DIAGNOSIS_{report.report_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        filepath = self.output_dir / filename
        
        md_content = f"""# 智能诊断报告

## 基本信息

| 项目 | 内容 |
|------|------|
| 报告编号 | {report.report_id} |
| 诊断编号 | {report.diagnosis_id} |
| 生成时间 | {report.created_at.strftime('%Y-%m-%d %H:%M:%S')} |
| 设备名称 | {report.device_name} |
| 操作人员 | {report.operator or '系统自动'} |

## 故障症状

{report.symptoms}

## 根因分析

**根本原因:** {report.root_cause}

**置信度:** {report.confidence*100:.1f}%

### 可能原因

{chr(10).join(f"{i+1}. {cause}" for i, cause in enumerate(report.possible_causes))}

## 建议措施

{chr(10).join(f"{i+1}. {action}" for i, action in enumerate(report.suggested_actions))}

## 推荐备件

{chr(10).join(f"- {part}" for part in report.spare_parts) if report.spare_parts else '无'}

---

*本报告由 Jamin Industrial Agent 智能诊断系统生成*
"""
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(md_content)
        
        logger.info(f"Markdown报告已生成: {filepath}")
        return str(filepath)
    
    def generate_json(self, report: DiagnosisReport) -> str:
        """生成JSON报告"""
        filename = f"DIAGNOSIS_{report.report_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = self.output_dir / filename
        
        data = {
            'report_id': report.report_id,
            'diagnosis_id': report.diagnosis_id,
            'created_at': report.created_at.isoformat(),
            'device_name': report.device_name,
            'symptoms': report.symptoms,
            'root_cause': report.root_cause,
            'confidence': report.confidence,
            'possible_causes': report.possible_causes,
            'suggested_actions': report.suggested_actions,
            'spare_parts': report.spare_parts,
            'references': report.references,
            'operator': report.operator
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"JSON报告已生成: {filepath}")
        return str(filepath)


# 使用示例
if __name__ == "__main__":
    # 创建示例报告
    report = DiagnosisReport(
        report_id="RPT001",
        diagnosis_id="DIAG001",
        created_at=datetime.now(),
        device_name="1#曝气池",
        symptoms="溶解氧浓度持续偏低，pH值正常，曝气盘疑似堵塞",
        root_cause="曝气盘部分堵塞导致曝气量不足",
        confidence=0.85,
        possible_causes=[
            "曝气盘堵塞",
            "风机故障",
            "DO传感器漂移"
        ],
        suggested_actions=[
            "检查并清洗曝气盘",
            "检查风机运行状态",
            "校准DO传感器"
        ],
        spare_parts=[
            "曝气盘 × 5",
            "风机滤网 × 1"
        ],
        references=[
            {"title": "污水处理厂曝气系统维护手册", "type": "manual"},
            {"title": "溶解氧异常处理案例", "type": "case"}
        ],
        similar_cases=[],
        trend_charts=[],
        operator="张工程师"
    )
    
    # 生成报告
    generator = ReportGenerator()
    
    # HTML报告
    html_path = generator.generate_html(report)
    print(f"HTML报告: {html_path}")
    
    # Markdown报告
    md_path = generator.generate_markdown(report)
    print(f"Markdown报告: {md_path}")
    
    # JSON报告
    json_path = generator.generate_json(report)
    print(f"JSON报告: {json_path}")
    
    # PDF报告 (需要reportlab)
    try:
        pdf_path = generator.generate_pdf(report)
        print(f"PDF报告: {pdf_path}")
    except Exception as e:
        print(f"PDF生成失败: {e}")

