# docker.nix
# Simplified Docker image build for clustbench-analysis with R
{ pkgs ? import <nixpkgs> {} }:

let
  # Import the core package definitions
  defs = import ./nix/default.nix { inherit pkgs; };
  
  # Extract variables we need
  inherit (defs) rPackageList;
  
  # Create a minimal Python environment
  pythonEnv = pkgs.python312.withPackages (ps: with ps; [
    pandas
    numpy
  ]);
  
  # Essential R packages that are needed by knitr
  essentialRDeps = with pkgs.rPackages; [
    xfun
    evaluate
    highr
    stringi
    stringr
    markdown
    yaml
    digest
  ];
  
  # Create a complete R environment with all packages and dependencies
  rEnvComplete = pkgs.rWrapper.override {
    packages = rPackageList ++ essentialRDeps;
  };
  
  # Create entrypoint script
  entrypointScript = pkgs.writeScript "entrypoint" ''
    #!/bin/bash
    set -e
    
    # Setup directories
    mkdir -p /app /tmp/Rtmp /tmp/R_libs_user
    chmod -R 777 /tmp
    
    # Set environment variables
    export TMPDIR="/tmp/Rtmp"
    export R_LIBS_USER="/tmp/R_libs_user"
    export R_LIBS="/usr/lib/R/library"
    
    # Print environment info
    echo "Clustbench Analysis Environment"
    echo "R: $(R --version | head -n 1)"
    echo "Python: $(python --version)"
    
    # Run the command
    if [ -z "$1" ]; then
      exec bash
    else
      exec "$@"
    fi
  '';

  # Build the Docker image
  dockerImage = pkgs.dockerTools.buildLayeredImage {
    name = "clustbench-analysis";
    tag = "latest";
    
    # Include all necessary contents
    contents = [
      rEnvComplete
      pythonEnv
      pkgs.bashInteractive
      pkgs.coreutils
      pkgs.gnugrep
      pkgs.gnused
    ];

    # Configuration
    config = {
      Env = [
        "R_HOME=${pkgs.R}/lib/R"
        "TMPDIR=/tmp/Rtmp"
        "R_LIBS_USER=/tmp/R_libs_user"
        "R_LIBS=/usr/lib/R/library"
        "LC_ALL=C.UTF-8"
        "LANG=C.UTF-8"
      ];
      WorkingDir = "/app";
      Entrypoint = [ "${entrypointScript}" ];
      Cmd = [ "/bin/bash" ];
    };

    # Setup actions
    extraCommands = ''
      # Create required directories with proper permissions
      mkdir -p tmp/Rtmp tmp/R_libs_user app
      chmod -R 777 tmp app tmp/Rtmp tmp/R_libs_user
      
      # Create a simple .Rprofile for the container
      cat > app/.Rprofile <<EOF
      # R environment for clustbench-analysis
      message("Loading R environment...")
      
      # Check if knitr and its dependencies are available
      if(requireNamespace("knitr", quietly=TRUE)) {
        message("✓ knitr package is available")
      } else {
        message("✗ knitr package is NOT available")
      }
      
      # List available packages
      list_packages <- function() {
        pkgs <- sort(rownames(installed.packages()))
        message("Available packages: ", paste(pkgs, collapse=", "))
      }
      
      # Set default CRAN mirror
      options(repos = c(CRAN = "https://cloud.r-project.org"))
      EOF
    '';
  };

  # Helper script
  runScript = pkgs.writeShellScriptBin "run-clustbench-docker" ''
    #!/bin/bash
    set -e
    
    # Add option to run with different network modes
    NETWORK_MODE="none"
    if [ "$1" == "--network=host" ]; then
      NETWORK_MODE="host"
      shift
    fi
    
    echo "Running with network mode: $NETWORK_MODE"
    docker run -it --rm --network=$NETWORK_MODE -v "$(pwd):/app" clustbench-analysis:latest "$@"
  '';
  
in {
  # Export the Docker image
  image = dockerImage;
  
  # Export the run script
  run = runScript;
}