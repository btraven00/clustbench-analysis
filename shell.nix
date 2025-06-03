# shell.nix
{ pkgs ? import <nixpkgs> {} }:

let
  # Define R packages in one place
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
  ];
  
  # Create custom R derivation with all packages
  rEnv = pkgs.buildEnv {
    name = "r-env";
    paths = [ 
      pkgs.R
      pkgs.rPackages.knitr
      pkgs.rPackages.rmarkdown
      pkgs.rPackages.tidyverse
      pkgs.rPackages.ggplot2
      pkgs.rPackages.arrow
      pkgs.rPackages.shiny
      pkgs.rPackages.flexdashboard
      pkgs.rPackages.DT
      pkgs.rPackages.patchwork
      pkgs.rPackages.kableExtra
      pkgs.rPackages.plotly
      pkgs.rPackages.scales
      pkgs.rPackages.testthat
    ];
  };
  
  # Create .Rprofile content with explicit libPaths setting
  rProfileContent = ''
    # .Rprofile created by Nix
    message("Loading Nix R environment...")
    
    # Set library paths explicitly
    .libPaths(c(
      "${pkgs.rPackages.knitr}/library",
      "${pkgs.rPackages.rmarkdown}/library",
      "${pkgs.rPackages.tidyverse}/library",
      "${pkgs.rPackages.ggplot2}/library",
      "${pkgs.rPackages.arrow}/library",
      "${pkgs.rPackages.shiny}/library",
      "${pkgs.rPackages.flexdashboard}/library",
      "${pkgs.rPackages.DT}/library",
      "${pkgs.rPackages.patchwork}/library",
      "${pkgs.rPackages.kableExtra}/library",
      "${pkgs.rPackages.plotly}/library",
      "${pkgs.rPackages.scales}/library",
      "${pkgs.rPackages.testthat}/library",
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
  '';
  
  # Create a shell script to launch RStudio with the right environment
  rstudioWrapper = pkgs.writeScriptBin "rstudio-wrapper" ''
    #!${pkgs.stdenv.shell}
    
    # Create .Rprofile if it doesn't exist
    if [ ! -f .Rprofile ]; then
      echo '${rProfileContent}' > .Rprofile
    fi
    
    # Launch RStudio with the right environment
    exec ${pkgs.rstudio}/bin/rstudio "$@"
  '';
  
in pkgs.mkShell {
  buildInputs = [
    pkgs.R
    pkgs.rstudio
    rEnv
    rstudioWrapper
  ];
  
  shellHook = ''
    # Create or update .Rprofile in the project directory
    echo '${rProfileContent}' > .Rprofile
    
    # Add information about the environment
    echo "=== R Environment Setup ==="
    echo "Custom .Rprofile has been created"
    echo ""
    echo "To start RStudio, run:"
    echo "  rstudio-wrapper"
    echo ""
    echo "To check if packages are working, run:"
    echo "  R -e 'library(knitr); cat(\"knitr loaded successfully\n\")'"
    echo ""
  '';
}
