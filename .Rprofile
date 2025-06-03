# .Rprofile created by Nix
message("Loading Nix R environment...")

# Set library paths explicitly
.libPaths(c(
  "/nix/store/g54xzq4v78jcyb3sqwg9w9yzz0fki6h0-r-knitr-1.49/library",
  "/nix/store/s2qsdrx20ah7pxf6wv226c5ykbabgq5l-r-rmarkdown-2.29/library",
  "/nix/store/4pcx08n8gvkzc3zpr1jsb9pb4fsmvv0p-r-tidyverse-2.0.0/library",
  "/nix/store/ikaqn370kwzp72gl0306awg1myjsqbcg-r-ggplot2-3.5.1/library",
  "/nix/store/wn9hkxyq4f3iccik80xn2bk8ahxj9cyd-r-arrow-20.0.0/library",
  "/nix/store/wim5m036p8mi4157rbmp2k4jb45g5pan-r-shiny-1.10.0/library",
  "/nix/store/zq5wb6layxzdsbzw55b8p48idr4jwjh9-r-flexdashboard-0.6.2/library",
  "/nix/store/c510hxn2a9m9xy9zfmm9azd9igw5fp49-r-DT-0.33/library",
  "/nix/store/8rxlwldj1nmmbg2ar1fnrl5hzviiz204-r-patchwork-1.3.0/library",
  "/nix/store/b4vz38ymnmim486k9g75k2p81cgry4q1-r-kableExtra-1.4.0/library",
  "/nix/store/xyxf5rh7d0h3cc0c1jzg08qggi9clh4l-r-plotly-4.10.4/library",
  "/nix/store/3h329ggwvl09z4s0i67n72xmc1nfs1lw-r-scales-1.3.0/library",
  "/nix/store/dwzpiw39yp977fng88p89kkfld0d7l73-r-testthat-3.2.3/library",
  .libPaths()
))

# Custom function to check installed packages - using try() to handle potential issues
list_nix_packages <- function() {
  # Safely check for installed packages
  result <- try({
    pkgs <- installed.packages()[, "Package"]
    message("Installed packages: ", paste(sort(pkgs), collapse=", "))
  }, silent = TRUE)
  
  if (inherits(result, "try-error")) {
    message("Unable to list packages: ", attr(result, "condition")$message)
  }
}

# Safely check if knitr is available
tryCatch({
  if (requireNamespace("knitr", quietly = TRUE)) {
    message("✓ knitr package is available")
  } else {
    message("✗ knitr package is NOT available")
  }
}, error = function(e) {
  message("Error checking for knitr: ", e$message)
})

message("Library paths: ", paste(.libPaths(), collapse=", "))

# Print info message
message("Nix R environment loaded! Use list_nix_packages() to see available packages")

