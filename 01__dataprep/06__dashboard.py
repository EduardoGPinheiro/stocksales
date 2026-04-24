"""Macroeconomic Dashboard for StockSales project."""
import pandas as pd
import plotly.express as px
import streamlit as st

# Page Configuration (SEO & Aesthetics)
st.set_page_config(
    page_title="Macroeconomic Insights | StockSales",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for Premium Look
st.markdown("""
    <style>
    .main {
        background-color: #f8f9fa;
    }
    [data-testid="stMetric"] {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        border: 1px solid #e5e7eb;
    }
    h1 {
        color: #1e3a8a;
        font-family: 'Inter', sans-serif;
        font-weight: 700;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_data
def load_data():
    """Load and prepare macroeconomic data from parquet.

    Returns:
        pd.DataFrame: Processed macroeconomic data.
    """
    path = "01__dataprep/data_treated/bacen_macroeconomics_vars.parquet"
    bacen_df = pd.read_parquet(path)

    bacen_df['time'] = pd.to_datetime(bacen_df['time'])
    bacen_df['value'] = pd.to_numeric(bacen_df['value'])

    return bacen_df


def main():
    """Execute the dashboard logic."""
    st.title("📊 Macroeconomic Dashboard")
    st.markdown("Real-time analysis of Selic, Ibovespa, and Dollar (USD).")

    try:
        bacen_df = load_data()
    except FileNotFoundError:
        st.error("Data file not found. Please run the dataprep scripts first.")
        return

    # Sidebar - Filters
    st.sidebar.header("Configuration")
    vars_available = sorted(bacen_df['variable'].unique().tolist())
    selected_vars = st.sidebar.multiselect(
        "Select Series",
        vars_available,
        default=vars_available
    )

    min_date = bacen_df['time'].min().date()
    max_date = bacen_df['time'].max().date()

    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # Filter Data
    if len(date_range) == 2:
        mask = (
            (bacen_df['time'].dt.date >= date_range[0]) &
            (bacen_df['time'].dt.date <= date_range[1]) &
            (bacen_df['variable'].isin(selected_vars))
        )

        bacen_filtered_df = bacen_df.loc[mask]
    else:
        bacen_filtered_df = pd.DataFrame()

    # Metrics Section
    if selected_vars:
        cols = st.columns(len(selected_vars))

        for i, var in enumerate(selected_vars):
            var_bacen = (
                bacen_filtered_df
                .loc[bacen_filtered_df['variable'] == var]
                .sort_values('time')
                .copy()
            )

            if len(var_bacen) >= 2:
                current_val = var_bacen['value'].iloc[-1]
                prev_val = var_bacen['value'].iloc[-2]
                delta = ((current_val - prev_val) / prev_val) * 100

                prefix = "R$ " if var == "dollar" else ""
                suffix = "%" if var == "selic" else " pts"

                cols[i].metric(
                    label=var.upper(),
                    value=f"{prefix}{current_val:,.2f}{suffix}",
                    delta=f"{delta:.2f}%"
                )

    st.divider()

    # Time Series Chart
    st.subheader("Historical Evolution")
    if not bacen_filtered_df.empty:
        fig = px.line(
            bacen_filtered_df,
            x='time',
            y='value',
            color='variable',
            template="plotly_white",
            color_discrete_sequence=px.colors.qualitative.Prism,
            labels={'time': 'Date', 'value': 'Value', 'variable': 'Indicator'}
        )

        fig.update_layout(
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=0, r=0, t=30, b=0)
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available for the selected filters.")


if __name__ == "__main__":
    main()
