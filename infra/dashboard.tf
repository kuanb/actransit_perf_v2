resource "google_monitoring_dashboard" "scraper" {
  dashboard_json = jsonencode({
    displayName = "actransit-scraper"
    mosaicLayout = {
      columns = 12
      tiles = [
        {
          xPos   = 0
          yPos   = 0
          width  = 6
          height = 4
          widget = {
            title = "Successful requests (last 1h)"
            scorecard = {
              timeSeriesQuery = {
                timeSeriesFilter = {
                  filter = "metric.type=\"run.googleapis.com/request_count\" resource.type=\"cloud_run_revision\" resource.labels.service_name=\"actransit-scraper\" metric.labels.response_code_class=\"2xx\""
                  aggregation = {
                    alignmentPeriod    = "3600s"
                    perSeriesAligner   = "ALIGN_DELTA"
                    crossSeriesReducer = "REDUCE_SUM"
                  }
                }
              }
              thresholds = []
            }
          }
        },
        {
          xPos   = 6
          yPos   = 0
          width  = 6
          height = 4
          widget = {
            title = "5xx errors (last 1h)"
            scorecard = {
              timeSeriesQuery = {
                timeSeriesFilter = {
                  filter = "metric.type=\"run.googleapis.com/request_count\" resource.type=\"cloud_run_revision\" resource.labels.service_name=\"actransit-scraper\" metric.labels.response_code_class=\"5xx\""
                  aggregation = {
                    alignmentPeriod    = "3600s"
                    perSeriesAligner   = "ALIGN_DELTA"
                    crossSeriesReducer = "REDUCE_SUM"
                  }
                }
              }
              thresholds = [
                { value = 1, color = "YELLOW", direction = "ABOVE" },
                { value = 5, color = "RED", direction = "ABOVE" },
              ]
            }
          }
        },
        {
          xPos   = 0
          yPos   = 4
          width  = 12
          height = 6
          widget = {
            title = "Request count by response class"
            xyChart = {
              chartOptions = { mode = "COLOR" }
              dataSets = [
                {
                  plotType       = "LINE"
                  legendTemplate = "$${metric.labels.response_code_class}"
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "metric.type=\"run.googleapis.com/request_count\" resource.type=\"cloud_run_revision\" resource.labels.service_name=\"actransit-scraper\""
                      aggregation = {
                        alignmentPeriod    = "60s"
                        perSeriesAligner   = "ALIGN_RATE"
                        crossSeriesReducer = "REDUCE_SUM"
                        groupByFields      = ["metric.label.response_code_class"]
                      }
                    }
                  }
                },
              ]
              timeshiftDuration = "0s"
              yAxis = {
                label = "req/s"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          xPos   = 0
          yPos   = 10
          width  = 12
          height = 6
          widget = {
            title = "Request latency"
            xyChart = {
              chartOptions = { mode = "COLOR" }
              dataSets = [
                {
                  plotType       = "LINE"
                  legendTemplate = "p50"
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "metric.type=\"run.googleapis.com/request_latencies\" resource.type=\"cloud_run_revision\" resource.labels.service_name=\"actransit-scraper\""
                      aggregation = {
                        alignmentPeriod    = "60s"
                        perSeriesAligner   = "ALIGN_PERCENTILE_50"
                        crossSeriesReducer = "REDUCE_MEAN"
                      }
                    }
                  }
                },
                {
                  plotType       = "LINE"
                  legendTemplate = "p95"
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "metric.type=\"run.googleapis.com/request_latencies\" resource.type=\"cloud_run_revision\" resource.labels.service_name=\"actransit-scraper\""
                      aggregation = {
                        alignmentPeriod    = "60s"
                        perSeriesAligner   = "ALIGN_PERCENTILE_95"
                        crossSeriesReducer = "REDUCE_MEAN"
                      }
                    }
                  }
                },
                {
                  plotType       = "LINE"
                  legendTemplate = "p99"
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "metric.type=\"run.googleapis.com/request_latencies\" resource.type=\"cloud_run_revision\" resource.labels.service_name=\"actransit-scraper\""
                      aggregation = {
                        alignmentPeriod    = "60s"
                        perSeriesAligner   = "ALIGN_PERCENTILE_99"
                        crossSeriesReducer = "REDUCE_MEAN"
                      }
                    }
                  }
                },
              ]
              yAxis = {
                label = "ms"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          xPos   = 0
          yPos   = 16
          width  = 6
          height = 6
          widget = {
            title = "Vehicles in flight"
            xyChart = {
              chartOptions = { mode = "COLOR" }
              dataSets = [
                {
                  plotType       = "LINE"
                  legendTemplate = "in_flight"
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "metric.type=\"custom.googleapis.com/actransit/vehicles_in_flight\" resource.type=\"global\""
                      aggregation = {
                        alignmentPeriod  = "60s"
                        perSeriesAligner = "ALIGN_MEAN"
                      }
                    }
                  }
                },
              ]
              yAxis = {
                label = "vehicles"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          xPos   = 6
          yPos   = 16
          width  = 6
          height = 6
          widget = {
            title = "Trips finalized per minute"
            xyChart = {
              chartOptions = { mode = "COLOR" }
              dataSets = [
                {
                  plotType       = "LINE"
                  legendTemplate = "trips/min"
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "metric.type=\"custom.googleapis.com/actransit/trips_finalized_per_minute\" resource.type=\"global\""
                      aggregation = {
                        alignmentPeriod  = "60s"
                        perSeriesAligner = "ALIGN_MEAN"
                      }
                    }
                  }
                },
              ]
              yAxis = {
                label = "trips"
                scale = "LINEAR"
              }
            }
          }
        },
      ]
    }
  })
}
