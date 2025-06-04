# default.nix
# Define packages and environments for the project
{ pkgs ? import <nixpkgs> {} }:

let
  # Essential R package dependencies - explicitly listed for clarity
  essentialRDeps = with pkgs.rPackages; [
    xfun       # Required by knitr
    evaluate   # Required by knitr
    highr      # Required by knitr
    stringi    # Required by stringr and other string operations
    stringr    # Required by knitr and many tidyverse packages
    markdown   # Required by knitr
    yaml       # Required by rmarkdown
    digest     # Required by many packages
  ];
in 
{
  # Core R packages for the project
  rPackageList = with pkgs.rPackages; [
    knitr
    rmarkdown
    tidyverse
    ggplot2
    arrow
    shiny
    flexdashboard
    DT
    patchwork
    kableExtra
    plotly
    scales
    testthat
    # add new dependencies here
  ] ++ essentialRDeps;
  
  # Create combined R environment with all packages
  rEnv = pkgs.buildEnv {
    name = "r-env";
    paths = [ pkgs.R ] ++ (with pkgs.rPackages; [
      knitr
      rmarkdown
      tidyverse
      ggplot2
      arrow
      shiny
      flexdashboard
      DT
      patchwork
      kableExtra
      plotly
      scales
      testthat
    ] ++ essentialRDeps);
  };
  
  # Create Python environment with pandas and numpy
  pythonEnv = pkgs.python312.withPackages (ps: with ps; [
    pandas
    numpy
  ]);
  
  # Create .Rprofile content with explicit library paths
  rProfileContent = ''
    # .Rprofile created by Nix
    message("Loading Nix R environment...")
    
    # Ensure temp directories exist and are writable
    if (Sys.getenv("R_LIBS_USER") == "") {
      Sys.setenv(R_LIBS_USER = file.path(tempdir(), "R_libs_user"))
      dir.create(Sys.getenv("R_LIBS_USER"), showWarnings = FALSE, recursive = TRUE)
    }
    
    # Set library paths
    .libPaths(c(.libPaths(), Sys.getenv("R_LIBS_USER")))
    
    # Show temp directory info
    message("R temp directory: ", tempdir())
    message("R user library: ", Sys.getenv("R_LIBS_USER"))
    
    # Check if temp directory is writable
    temp_writable <- tryCatch({
      test_file <- file.path(tempdir(), "test_write.tmp")
      writeLines("test", test_file)
      file.exists(test_file)
    }, error = function(e) {
      message("Warning: Temp directory not writable: ", e$message)
      FALSE
    })
    
    if (temp_writable) {
      message("✓ R temp directory is writable")
    } else {
      message("✗ R temp directory is NOT writable")
    }
    
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
  '';
}