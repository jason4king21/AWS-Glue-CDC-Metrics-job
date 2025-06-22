import streamlit as st
import pandas as pd
import plotly.express as px
import s3fs
import pyarrow.parquet as pq

st.set_page_config(layout="wide")
st.title("📈 Business Insights Dashboard")

# ----------------- Constants -----------------
BUCKET = "jk-business-insights-assessment"


# ----------------- Helper Functions -----------------

def load_parquet_from_s3(bucket, prefix):
    base_path = f"s3://{bucket}/{prefix}"
    fs = s3fs.S3FileSystem(anon=False)
    paths = fs.glob(f"{base_path}**")
    valid_files = [p for p in paths if p.endswith(".parquet") or p.endswith(".snappy.parquet")]
    return pd.concat([pd.read_parquet(f"s3://{p}", filesystem=fs) for p in valid_files], ignore_index=True)





# ----------------- Tabs -----------------
tabs = st.tabs([
    "💰 CLV Segmentation",
    "🧱 RFM Segments",
    "📉 Churn Risk",
    "📊 Sales Trends",
    "🎁 Loyalty Impact",
    "📍 Location Performance",
    "🏷️ Discount Effectiveness"
])

# ----------------- Tab 1: CLV -----------------
with tabs[0]:
    st.header("💰 Customer Lifetime Value (CLV)")
    try:
        df_snapshot = load_parquet_from_s3(BUCKET, "data/gold/mart_customer_ltv_snapshot/")
        df_segment = load_parquet_from_s3(BUCKET, "data/gold/mart_customer_clv_segment/")

        # --- Metric Summary ---
        total_customers = df_segment["USER_ID"].nunique()
        avg_ltv = df_segment["CUMULATIVE_LTV"].mean()
        high_value_customers = df_segment[df_segment["CLV_GROUP"] == "High"]["USER_ID"].nunique()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Customers", f"{total_customers:,}")
        col2.metric("Avg CLV", f"${avg_ltv:,.2f}")
        col3.metric("High-Value Customers", f"{high_value_customers:,}")

        # --- Count of customers per CLV group ---
        st.subheader("CLV Segment Distribution (Bar Chart)")

        segment_counts = df_segment["CLV_GROUP"].value_counts().reset_index()
        segment_counts.columns = ["CLV_GROUP", "Customer Count"]

        fig = px.bar(
            segment_counts,
            x="Customer Count",
            y="CLV_GROUP",
            orientation="h",
            color="CLV_GROUP",
            title="Number of Customers in Each CLV Segment",
            color_discrete_sequence=px.colors.qualitative.Set1
        )

        st.plotly_chart(fig, use_container_width=True)

        # --- Segment Table ---
        st.subheader("CLV Segment Table")
        st.dataframe(df_segment.sort_values("CUMULATIVE_LTV", ascending=False).reset_index(drop=True), use_container_width=True)

    except Exception as e:
        st.error(f"Error loading CLV data: {e}")


# ----------------- Tab 2: RFM -----------------
with tabs[1]:
    st.header("🧱 RFM Segmentation")

    try:
        df = load_parquet_from_s3(BUCKET, "data/gold/mart_customer_rfm/")

        if df.empty:
            st.warning("RFM data is empty. Please check the pipeline.")
        else:
            # --- Summary Metrics ---
            total_customers = df["USER_ID"].nunique()
            avg_recency = df["RECENCY"].mean()
            avg_frequency = df["FREQUENCY"].mean()
            avg_monetary = df["MONETARY"].mean()

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Customers", f"{total_customers:,}")
            col2.metric("Avg Recency (days)", f"{avg_recency:.1f}")
            col3.metric("Avg Frequency", f"{avg_frequency:.2f}")
            col4.metric("Avg Monetary", f"${avg_monetary:,.2f}")

            # --- Segment Distribution ---
            st.subheader("Segment Distribution")
            seg_counts = df["SEGMENT"].value_counts().reset_index()
            seg_counts.columns = ["SEGMENT", "COUNT"]
            fig = px.bar(
                seg_counts,
                x="SEGMENT",
                y="COUNT",
                color="SEGMENT",
                title="Customer Segments (RFM)",
                labels={"COUNT": "Customer Count"},
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)

            # --- Segment-level Averages ---
            st.subheader("Segment-Level Summary")
            segment_summary = df.groupby("SEGMENT").agg({
                "USER_ID": "count",
                "RECENCY": "mean",
                "FREQUENCY": "mean",
                "MONETARY": "mean"
            }).rename(columns={"USER_ID": "CUSTOMERS"}).round(2).reset_index()
            st.dataframe(segment_summary, use_container_width=True)

            # --- Raw Table ---
            st.subheader("Raw RFM Data")
            st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading RFM data: {e}")


# ----------------- Tab 3: Churn -----------------
with tabs[2]:
    st.header("📉 Churn Risk Indicators")

    try:
        df = load_parquet_from_s3(BUCKET, "data/gold/mart_customer_churn_profile/")

        if df.empty:
            st.warning("No churn data found.")
        else:
            # Summary Metrics
            total_customers = df["USER_ID"].nunique()
            avg_days_since = df["DAYS_SINCE_LAST_ORDER"].mean()
            avg_gap = df["AVG_ORDER_GAP_DAYS"].mean()
            avg_change = df["PCT_SPEND_CHANGE"].fillna(0).mean()

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Customers", f"{total_customers:,}")
            col2.metric("Avg Days Since Last Order", f"{avg_days_since:.1f}")
            col3.metric("Avg Order Gap (Days)", f"{avg_gap:.1f}")
            col4.metric("% Spend Change (Avg)", f"{avg_change:.1f}%")

            # Risk Distribution
            st.subheader("Risk Segment Distribution")
            risk_counts = df["CHURN_RISK_TAG"].value_counts().reset_index()
            risk_counts.columns = ["CHURN_RISK_TAG", "Customer Count"]

            fig = px.bar(
                risk_counts,
                x="CHURN_RISK_TAG",
                y="Customer Count",
                color="CHURN_RISK_TAG",
                title="Customer Distribution by Churn Risk Tag"
            )
            st.plotly_chart(fig, use_container_width=True)

            # Scatter Plot
            st.subheader("Churn Profile Scatter")
            fig = px.scatter(
                df,
                x="AVG_ORDER_GAP_DAYS",
                y="DAYS_SINCE_LAST_ORDER",
                color="CHURN_RISK_TAG",
                size=df["SPEND_LAST_30"].fillna(1),  # 🔧 Fix: Replace NaN with 1 for visibility
                hover_data=["USER_ID", "PCT_SPEND_CHANGE"],
                title="Customer Order Behavior"
            )
            st.plotly_chart(fig, use_container_width=True)

            # Raw Table
            st.subheader("Raw Churn Profile Data")
            st.dataframe(df.sort_values("DAYS_SINCE_LAST_ORDER", ascending=False), use_container_width=True)

    except Exception as e:
        st.error(f"Error loading churn data: {e}")

# ----------------- Tab 4: Sales Trends -----------------
with tabs[3]:
    st.header("📊 Sales Trends & Seasonality")

    try:
        # Load all time series
        df_daily = load_parquet_from_s3(BUCKET, "data/gold/mart_sales_trends/daily/")
        df_weekly = load_parquet_from_s3(BUCKET, "data/gold/mart_sales_trends/weekly/")
        df_monthly = load_parquet_from_s3(BUCKET, "data/gold/mart_sales_trends/monthly/")
        df_hourly = load_parquet_from_s3(BUCKET, "data/gold/mart_sales_trends/hourly/")

        st.subheader("🗓️ Daily Revenue")
        df_daily["CREATION_DATE"] = pd.to_datetime(df_daily["CREATION_DATE"])
        fig = px.line(df_daily.groupby("CREATION_DATE")["DAILY_REVENUE"].sum().reset_index(),
                      x="CREATION_DATE", y="DAILY_REVENUE", title="Total Daily Revenue")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📅 Weekly Revenue by Category")
        fig = px.bar(df_weekly, x="YEAR_WEEK", y="WEEKLY_REVENUE", color="ITEM_CATEGORY",
                     title="Weekly Revenue by Item Category", barmode="group")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("🗓️ Monthly Revenue by Restaurant")
        fig = px.bar(df_monthly, x="YEAR_MONTH", y="MONTHLY_REVENUE", color="RESTAURANT_ID",
                     title="Monthly Revenue by Restaurant", barmode="group")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("⏰ Hourly Revenue Distribution")
        fig = px.box(df_hourly, x="HOUR_OF_DAY", y="HOURLY_REVENUE", color="ITEM_CATEGORY",
                     title="Revenue by Hour of Day (Box Plot)")
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("📋 Show Raw Aggregates"):
            st.write("Daily Revenue", df_daily)
            st.write("Weekly Revenue", df_weekly)
            st.write("Monthly Revenue", df_monthly)
            st.write("Hourly Revenue", df_hourly)

    except Exception as e:
        st.error(f"Error loading sales trend data: {e}")


# ----------------- Tab 5: Loyalty -----------------
with tabs[4]:
    st.header("🎁 Loyalty Program Impact")

    try:
        df = load_parquet_from_s3(BUCKET, "data/gold/mart_loyalty_program_impact/")
        df["IS_LOYALTY"] = df["IS_LOYALTY"].map({True: "Loyalty Member", False: "Non-Member"})

        # Summary KPIs
        loyalty = df[df["IS_LOYALTY"] == "Loyalty Member"]
        non_loyalty = df[df["IS_LOYALTY"] == "Non-Member"]

        col1, col2, col3 = st.columns(3)
        col1.metric("Avg Spend (Loyalty)", f"${loyalty['AVG_SPEND_PER_CUSTOMER'].values[0]:,.2f}")
        col2.metric("Repeat Rate (Loyalty)", f"{loyalty['REPEAT_ORDER_RATE'].values[0]*100:.1f}%")
        col3.metric("Customers (Loyalty)", f"{loyalty['NUM_CUSTOMERS'].values[0]:,}")

        col4, col5, col6 = st.columns(3)
        col4.metric("Avg Spend (Non-Member)", f"${non_loyalty['AVG_SPEND_PER_CUSTOMER'].values[0]:,.2f}")
        col5.metric("Repeat Rate (Non-Member)", f"{non_loyalty['REPEAT_ORDER_RATE'].values[0]*100:.1f}%")
        col6.metric("Customers (Non-Member)", f"{non_loyalty['NUM_CUSTOMERS'].values[0]:,}")

        # Visualization: Compare Avg Spend and Repeat Rate
        st.subheader("📊 Loyalty Impact Comparison")
        bar_df = df.rename(columns={
            "AVG_SPEND_PER_CUSTOMER": "Avg Spend",
            "REPEAT_ORDER_RATE": "Repeat Rate"
        })

        fig = px.bar(bar_df, x="IS_LOYALTY", y="Avg Spend", color="IS_LOYALTY",
                     title="Average Spend by Loyalty Status")
        st.plotly_chart(fig, use_container_width=True)

        fig = px.bar(bar_df, x="IS_LOYALTY", y="Repeat Rate", color="IS_LOYALTY",
                     title="Repeat Order Rate by Loyalty Status")
        st.plotly_chart(fig, use_container_width=True)

        # Raw data table
        st.subheader("📋 Raw Summary")
        st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading loyalty data: {e}")


# ----------------- Tab 6: Location Performance -----------------
with tabs[5]:
    st.header("📍 Top-Performing Locations")

    try:
        df = load_parquet_from_s3(BUCKET, "data/gold/mart_location_performance/")

        if df.empty:
            st.warning("No location performance data found.")
        else:
            # Top Metrics
            top_location = df.loc[df["REVENUE_RANK"] == 1]
            col1, col2, col3 = st.columns(3)
            col1.metric("Top Location", f"ID {top_location['RESTAURANT_ID'].values[0]}")
            col2.metric("Total Revenue", f"${top_location['TOTAL_REVENUE'].values[0]:,.2f}")
            col3.metric("Orders per Day", f"{top_location['ORDERS_PER_DAY'].values[0]:.2f}")

            # Bar Chart: Total Revenue by Location
            st.subheader("🏆 Total Revenue by Location")
            fig = px.bar(
                df.sort_values("TOTAL_REVENUE", ascending=False),
                x="RESTAURANT_ID",
                y="TOTAL_REVENUE",
                color="RESTAURANT_ID",
                title="Total Revenue by Restaurant Location",
                labels={"RESTAURANT_ID": "Restaurant", "TOTAL_REVENUE": "Revenue"}
            )
            st.plotly_chart(fig, use_container_width=True)

            # Scatter: Orders per Day vs Revenue
            st.subheader("📈 Orders vs Revenue")
            fig = px.scatter(
                df,
                x="ORDERS_PER_DAY",
                y="TOTAL_REVENUE",
                size="NUM_ORDERS",
                color="RESTAURANT_ID",
                hover_data=["RESTAURANT_ID", "AVG_ORDER_VALUE"],
                title="Orders Per Day vs Total Revenue"
            )
            st.plotly_chart(fig, use_container_width=True)

            # Raw Table
            st.subheader("📋 Location Performance Table")
            st.dataframe(df.sort_values("REVENUE_RANK"), use_container_width=True)

    except Exception as e:
        st.error(f"Error loading location performance data: {e}")

# ----------------- Tab 7: Discount Effectiveness -----------------
with tabs[6]:
    st.header("🏷️ Pricing & Discount Effectiveness")

    try:
        df = load_parquet_from_s3(BUCKET, "data/gold/mart_discount_effectiveness/")
        df["IS_DISCOUNTED_ORDER"] = df["IS_DISCOUNTED_ORDER"].map({"Yes": "Discounted", "No": "Full Price"})

        # Summary Metrics
        discounted = df[df["IS_DISCOUNTED_ORDER"] == "Discounted"]
        full_price = df[df["IS_DISCOUNTED_ORDER"] == "Full Price"]

        col1, col2, col3 = st.columns(3)
        col1.metric("Orders (Discounted)", f"{discounted['NUM_ORDERS'].values[0]:,}")
        col2.metric("Revenue (Discounted)", f"${discounted['TOTAL_REVENUE'].values[0]:,.2f}")
        col3.metric("Avg Order Value", f"${discounted['AVG_ORDER_VALUE'].values[0]:,.2f}")

        col4, col5, col6 = st.columns(3)
        col4.metric("Orders (Full Price)", f"{full_price['NUM_ORDERS'].values[0]:,}")
        col5.metric("Revenue (Full Price)", f"${full_price['TOTAL_REVENUE'].values[0]:,.2f}")
        col6.metric("Avg Order Value", f"${full_price['AVG_ORDER_VALUE'].values[0]:,.2f}")

        # Revenue Comparison Chart
        st.subheader("💰 Revenue Comparison")
        fig = px.bar(
            df,
            x="IS_DISCOUNTED_ORDER",
            y="TOTAL_REVENUE",
            color="IS_DISCOUNTED_ORDER",
            title="Total Revenue: Discounted vs Full Price",
            labels={"IS_DISCOUNTED_ORDER": "Discount Status", "TOTAL_REVENUE": "Revenue"}
        )
        st.plotly_chart(fig, use_container_width=True)

        # Average Order Value Comparison
        st.subheader("🧾 Average Order Value")
        fig = px.bar(
            df,
            x="IS_DISCOUNTED_ORDER",
            y="AVG_ORDER_VALUE",
            color="IS_DISCOUNTED_ORDER",
            title="Avg Order Value: Discounted vs Full Price",
            labels={"IS_DISCOUNTED_ORDER": "Discount Status", "AVG_ORDER_VALUE": "Avg Order Value"}
        )
        st.plotly_chart(fig, use_container_width=True)

        # Show Raw Data
        st.subheader("📋 Discount Summary Table")
        st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading discount effectiveness data: {e}")

