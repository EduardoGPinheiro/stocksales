# app.R
library(shiny)
library(dplyr)
library(ggplot2)
library(plotly)       # interactive plots
library(arrow)
library(reshape2)
library(cowplot)
library(scales)
library(showtext)

# ---- Font ----
font_add_google("Montserrat", "montserrat")
showtext_auto()

# ---- Colors ----
blue <- rgb(32, 114, 184, maxColorValue = 255)
red <- rgb(237, 0, 0, maxColorValue = 255)

# ---- UI ----
ui <- fluidPage(
  titlePanel("B3 Time Series Analysis"),
  sidebarLayout(
    sidebarPanel(
      fileInput("file", "Upload modelmatrix.parquet",
                accept = ".parquet"),
      selectInput("plot_type", "Select Plot:",
                  choices = c("Raw Returns" = "raw",
                              "Cumulative Returns" = "cumulative"),
                  selected = "raw"),
      checkboxGroupInput("series_select", "Select Series:",
                         choices = NULL, selected = NULL),
      width = 3
    ),
    mainPanel(
      plotlyOutput("time_series_plot", height = "600px"),
      width = 9
    )
  )
)

# ---- SERVER ----
server <- function(input, output, session) {
  
  # Reactive: load parquet file
  model_matrix <- reactive({
    req(input$file)
    read_parquet(input$file$datapath)
  })
  
  # Reactive: tidy returns
  returns_df <- reactive({
    df <- model_matrix() %>%
      reshape2::melt(id.vars = "time") %>%
      mutate(cumulative_returns = exp(value)) %>%
      group_by(variable) %>%
      mutate(cumulative_returns = cumprod(cumulative_returns) - 1) %>%
      ungroup()
    df
  })
  
  # Update series selection
  observeEvent(returns_df(), {
    vars <- unique(returns_df()$variable)
    updateCheckboxGroupInput(session, "series_select",
                             choices = vars,
                             selected = vars[1:min(5, length(vars))])
  })
  
  # Render plot
  output$time_series_plot <- renderPlotly({
    req(input$series_select)
    df <- returns_df() %>%
      filter(variable %in% input$series_select)
    
    p <- ggplot(df, aes(x = time,
                        y = ifelse(input$plot_type == "raw", value, cumulative_returns),
                        color = variable,
                        group = variable)) +
      geom_line(size = 1) +
      theme_minimal(base_family = "montserrat") +
      labs(x = "Time",
           y = ifelse(input$plot_type == "raw", "Raw Returns", "Cumulative Returns"),
           color = "Series") +
      theme(
        legend.position = "bottom",
        plot.title = element_text(size = 16, face = "bold"),
        axis.title = element_text(size = 14)
      )
    
    ggplotly(p)
  })
}

# ---- RUN APP ----
shinyApp(ui, server)
