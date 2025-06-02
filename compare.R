
# Load required libraries
library(arrow)
library(dplyr)
library(ggplot2)
library(ggrepel)  # For non-overlapping labels
library(tidyr)    # For data manipulation

# Define helper functions
read_dataset <- function(file_path) {
  df <- read_parquet(file_path)
  # Add combo key for easier joining
  df$combo_key <- paste(df$dataset_generator, 
                        df$dataset_name, 
                        df$method, 
                        df$metric, 
                        sep = "|")
  return(df)
}

# Read the parquet files
d.apptainer <- read_dataset("clustbench_aggregated_scores_apptainer_202505301205.parquet")
d.conda <- read_dataset("clustbench_aggregated_scores_conda_202506022012.parquet")

# Function to prepare data for comparison
prepare_comparison <- function(df1, df2, name1 = "apptainer", name2 = "conda") {
  comparison <- df1 %>%
    select(combo_key, score, execution_time_seconds, duplicate_k_anomaly) %>%
    rename_with(~ paste0(., "_", name1), c("score", "execution_time_seconds", "duplicate_k_anomaly")) %>%
    inner_join(
      df2 %>%
        select(combo_key, score, execution_time_seconds, duplicate_k_anomaly) %>%
        rename_with(~ paste0(., "_", name2), c("score", "execution_time_seconds", "duplicate_k_anomaly")),
      by = "combo_key"
    )
  
  # Split the combo_key into components
  comparison <- comparison %>%
    separate(combo_key, into = c("dataset_gen", "dataset_name", "method", "metric"), 
             sep = "\\|", remove = FALSE)
  
  # Calculate differences and ratios
  score_col1 <- paste0("score_", name1)
  score_col2 <- paste0("score_", name2)
  time_col1 <- paste0("execution_time_seconds_", name1)
  time_col2 <- paste0("execution_time_seconds_", name2)
  anomaly_col1 <- paste0("duplicate_k_anomaly_", name1)
  anomaly_col2 <- paste0("duplicate_k_anomaly_", name2)
  
  comparison <- comparison %>%
    mutate(
      score_diff = .data[[score_col1]] - .data[[score_col2]],
      score_pct_diff = 100 * abs(score_diff) / ((.data[[score_col1]] + .data[[score_col2]]) / 2),
      time_ratio = .data[[time_col1]] / .data[[time_col2]],
      any_anomaly = .data[[anomaly_col1]] | .data[[anomaly_col2]]
    )
  
  return(comparison)
}

# Join the datasets and calculate differences
comparison <- prepare_comparison(d.apptainer, d.conda)

# Rename for compatibility with existing code
names(comparison) <- gsub("execution_time_seconds_apptainer", "time_apptainer", names(comparison))
names(comparison) <- gsub("execution_time_seconds_conda", "time_conda", names(comparison))
names(comparison) <- gsub("duplicate_k_anomaly_apptainer", "anomaly_apptainer", names(comparison))
names(comparison) <- gsub("duplicate_k_anomaly_conda", "anomaly_conda", names(comparison))

# Create a column to highlight differences above threshold (>1% difference)
comparison$above_threshold <- comparison$score_pct_diff > 1
comparison$label <- ifelse(comparison$above_threshold, comparison$combo_key, NA)

# Summary statistics
cat("Summary of score differences:\n")
summary(comparison$score_diff)

cat("\nSummary of percentage differences:\n")
summary(comparison$score_pct_diff)

cat("\nNumber of differences above threshold (>1%):", sum(comparison$above_threshold), "\n")

# Function to create score comparison plot
create_score_plot <- function(comparison, threshold = 1, 
                              x_label = "Score (Conda)", 
                              y_label = "Score (Apptainer)",
                              output_file = "score_comparison.png") {
  p <- ggplot(comparison, aes(x = score_conda, y = score_apptainer)) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "darkgray") +
    geom_point(aes(color = above_threshold, size = score_pct_diff, shape = any_anomaly)) +
    scale_color_manual(values = c("TRUE" = "red", "FALSE" = "blue")) +
    scale_shape_manual(values = c("TRUE" = 17, "FALSE" = 16)) +
    geom_text_repel(aes(label = label), size = 3, max.overlaps = 15) +
    labs(
      title = "Comparison of Clustering Scores: Apptainer vs Conda",
      subtitle = paste0("Points on the diagonal line have identical scores. Difference threshold: >", threshold, "%"),
      x = x_label,
      y = y_label,
      color = paste0("Diff >", threshold, "%"),
      size = "% Difference",
      shape = "Has Anomaly"
    ) +
    theme_minimal() +
    coord_equal() +  # Equal scaling on both axes
    theme(legend.position = "bottom")
  
  print(p)
  ggsave(output_file, p, width = 10, height = 8, dpi = 300)
  return(p)
}

# Create the main comparison plot
p <- create_score_plot(comparison)

# Function to create faceted plots by dataset
create_facet_plot <- function(comparison, facet_var = "dataset_name",
                              output_file = "score_comparison_by_dataset.png") {
  p_facet <- ggplot(comparison, aes(x = score_conda, y = score_apptainer)) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "darkgray") +
    geom_point(aes(color = score_pct_diff)) +
    scale_color_gradient(low = "blue", high = "red") +
    facet_wrap(as.formula(paste("~", facet_var)), scales = "free") +
    labs(
      title = paste0("Comparison by ", facet_var, ": Apptainer vs Conda"),
      x = "Score (Conda)",
      y = "Score (Apptainer)",
      color = "% Difference"
    ) +
    theme_minimal()
  
  print(p_facet)
  ggsave(output_file, p_facet, width = 12, height = 10, dpi = 300)
  return(p_facet)
}

# Function to create execution time comparison plot
create_time_plot <- function(comparison, output_file = "time_comparison.png") {
  p_time <- ggplot(comparison, aes(x = method, y = time_ratio)) +
    geom_hline(yintercept = 1, linetype = "dashed", color = "darkgray") +
    geom_boxplot(aes(fill = method)) +
    scale_y_log10() +  # Log scale for better visualization of ratios
    labs(
      title = "Execution Time Ratio: Apptainer / Conda",
      subtitle = "Values > 1 mean Apptainer is slower, < 1 mean Apptainer is faster",
      x = "Method",
      y = "Time Ratio (log scale)"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1),
          legend.position = "none")
  
  print(p_time)
  ggsave(output_file, p_time, width = 12, height = 6, dpi = 300)
  return(p_time)
}

# Create the faceted and time comparison plots
p_facet <- create_facet_plot(comparison)
p_time <- create_time_plot(comparison)

# Create additional facets by method (if needed)
p_facet_method <- create_facet_plot(comparison, "method", "score_comparison_by_method.png")

# Function to extract and save significant differences
analyze_differences <- function(comparison, threshold = 1, 
                               output_file = "differences_above_threshold.csv") {
  differences_above_threshold <- comparison %>%
    filter(score_pct_diff > threshold) %>%
    select(dataset_gen, dataset_name, method, metric, 
           score_apptainer, score_conda, score_diff, score_pct_diff, 
           time_apptainer, time_conda, time_ratio,
           any_anomaly) %>%
    arrange(desc(score_pct_diff))
  
  if (nrow(differences_above_threshold) > 0) {
    print(paste0("Found ", nrow(differences_above_threshold), " differences with >", 
                threshold, "% score difference:"))
    print(head(differences_above_threshold, 20))
    write.csv(differences_above_threshold, output_file, row.names = FALSE)
  } else {
    print(paste0("No differences found above ", threshold, "% threshold"))
  }
  
  return(differences_above_threshold)
}

# Generate reports for different thresholds
differences_1pct <- analyze_differences(comparison, 1, "differences_above_1pct.csv")
differences_05pct <- analyze_differences(comparison, 0.5, "differences_above_05pct.csv")
differences_01pct <- analyze_differences(comparison, 0.1, "differences_above_01pct.csv")

# Save the full comparison results
write.csv(comparison, "apptainer_vs_conda_comparison.csv", row.names = FALSE)

# Generate summary statistics for different metrics
method_summary <- comparison %>%
  group_by(method) %>%
  summarise(
    mean_time_ratio = mean(time_ratio, na.rm = TRUE),
    median_time_ratio = median(time_ratio, na.rm = TRUE),
    mean_score_diff = mean(score_diff, na.rm = TRUE),
    mean_abs_score_diff = mean(abs(score_diff), na.rm = TRUE),
    max_abs_score_diff = max(abs(score_diff), na.rm = TRUE),
    n = n()
  ) %>%
  arrange(desc(mean_abs_score_diff))

print("Summary by method:")
print(method_summary)
write.csv(method_summary, "method_performance_summary.csv", row.names = FALSE)
