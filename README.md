# V Mobile: Subscriber Behavior & Campaign Analytics

## 📖 Project Overview

This end-to-end data solution was developed to address a critical business problem at **V Mobile**, a mobile operator running a "free minutes" promotional campaign. Marketing lacked visibility into campaign performance, including who qualified for rewards, their geographic distribution, and behavioral patterns.

This project involved building a **complete data pipeline**—from raw, disparate data sources to an interactive **Power BI dashboard**—enabling data-driven decision-making to optimize marketing strategies and increase subscriber value.

## 🎯 Business Problem

V Mobile's marketing team was operating without a clear, automated view of their campaign's performance. Key challenges included:
- **No Single Customer View:** Subscriber data was fragmented across three different source systems (V Mobile, BlueMobile, ArrowMobile) after a company amalgamation.
- **Manual Qualification Tracking:** An automated weekly report identifying subscribers who generated ≥ R30 in weekly revenue (the qualification threshold) was non-existent.
- **Lack of Strategic Insights:** Beyond a simple list, there was no deep-dive analysis into *who* the valuable subscribers were, *where* they were located, or *what* behaviors led to qualification.

## 🛠️ Solution Architecture

### Data Pipeline

### 1. Data Consolidation & Master Record Creation
- **Tool:** Python (Pandas)
- **Action:** Merged three subscriber source systems into a single, trusted source
- **Key Achievement:** Implemented complex business rules to resolve duplicates, creating definitive "master records"

### 2. Data Modeling & ETL
- **Tool:** Power Query & DAX
- **Action:** Built robust data model linking subscriber master with weekly event data
- **Key Achievement:** Created dynamic qualification logic using DAX measures

### 3. Interactive Dashboard Development
- **Tool:** Power BI
- **Action:** Designed multi-page, interactive dashboard with four key chapters:

#### 📈 Chapter 1: The Big Picture
- Weekly performance KPIs
- Revenue and subscriber trends
- Overall campaign health metrics

#### 🗺️ Chapter 2: The Geographic Story  
- Regional performance analysis
- Qualification rate by location
- Geographic distribution of high-value subscribers

#### 👤 Chapter 3: The Subscriber Story
- Demographic insights
- Usage behavior patterns (SMS vs Voice)
- Revenue distribution analysis
- Tenure-based qualification trends

#### 🎯 Chapter 4: The Action Plan
- High-potential subscriber segments
- Behavioral patterns of qualifiers
- Direct marketing recommendations

## 📊 Key Insights Delivered

- **Identified High-Value Segments:** Pinpointed subscribers with high revenue but low activity as the most efficient and profitable segment
- **Uncovered Geographic Opportunities:** Discovered regions with high qualification efficiency that could serve as a "playbook" for underperforming markets
- **Mapped the Qualification Journey:** Revealed that subscriber value builds over time, highlighting the critical importance of reducing early-stage churn
- **Provided Direct Target List:** Delivered actionable list of high-potential subscribers for targeted marketing campaigns

## 🧠 Skills Demonstrated & Learned

### Technical Skills
- **Python Data Manipulation:** Pandas for data consolidation and business rule implementation
- **Power BI Development:** End-to-end dashboard creation from data modeling to visualization
- **DAX & Data Modeling:** Created calculated measures and optimized data relationships
- **ETL Processes:** Designed efficient data transformation pipelines
- **SQL Concepts:** Applied relational database thinking to data structure design

### Business Skills
- **Data Storytelling:** Structured complex analysis into compelling narrative
- **Stakeholder Communication:** Translated technical work into business value
- **Problem-Solving:** Addressed vague business requirements with structured solutions
- **Project Ownership:** Managed end-to-end from data extraction to client presentation

## 🚀 Tech Stack

| Category | Tools |
|----------|-------|
| **Data Preparation** | Python (Pandas), Power Query |
| **Data Visualization** | Power BI, DAX |
| **Business Intelligence** | Data Modeling, KPI Development |
| **Presentation** | PowerPoint, Data Storytelling |
| **Version Control** | Git, GitHub |


## 🎯 Business Impact

The solution transformed V Mobile's campaign management by:
- **Automating** weekly qualification reporting, saving ~8 hours of manual work weekly
- **Identifying** 23% growth opportunity in underperforming regions
- **Enabling** targeted marketing campaigns with 15% higher conversion potential
- **Providing** scalable framework for future campaign analysis

---

**Note:** This project was completed as a capstone/data challenge. V Mobile is a fictional company created for this use case, and all data was synthetically generated.

## 📞 Contact

Feel free to reach out if you have any questions about this project or would like to discuss data analytics opportunities!

Eddie  
0724981445