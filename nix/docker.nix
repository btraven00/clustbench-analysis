# docker.nix
# Creates a Docker image with R and Python environments for clustbench
{ pkgs ? import <nixpkgs> {} }:

let
  # Import package definitions from default.nix
  defs = import ./default.nix { inherit pkgs; };
  
  # Extract variables from the imported definitions
  inherit (defs) rPackageList rEnv pythonEnv;

  # Create simplified R profile for Docker as a string
  rProfileContent = ''
    message("Loading Docker R environment for clustbench-analysis...")
    
    # Create R temp directories
    tryCatch({
      temp_dir <- "/tmp/Rtmp"
      lib_dir <- "/tmp/R_libs_user"
      
      if (!dir.exists(temp_dir)) dir.create(temp_dir, recursive = TRUE)
      if (!dir.exists(lib_dir)) dir.create(lib_dir, recursive = TRUE)
      
      Sys.setenv(TMPDIR = temp_dir)
      Sys.setenv(R_LIBS_USER = lib_dir)
      
      message("R temp directory: ", tempdir())
    }, error = function(e) {
      message("Error setting up directories: ", e$message)
    })
    
    # Set CRAN mirror
    options(repos = c(CRAN = "https://cloud.r-project.org"))
    options(save = "no")
  '';

  # Create entrypoint script
  entrypointScript = pkgs.writeScriptBin "entrypoint" (builtins.readFile ./docker-entrypoint.sh);

in pkgs.dockerTools.buildLayeredImage {
  name = "clustbench-analysis";
  tag = "latest";
  
  # Set up the image contents
  contents = [
    pkgs.bashInteractive
    pkgs.coreutils
    pkgs.curl
    pkgs.gnugrep
    pkgs.gnused
    pkgs.gzip
    pkgs.findutils
    pkgs.which
    pkgs.R
    rEnv
    pythonEnv
    entrypointScript
  ];

  # Set up the environment
  config = {
    WorkingDir = "/app";
    Env = [
      "R_HOME=${pkgs.R}/lib/R"
      "PYTHONPATH=${pythonEnv}/lib/python3.12/site-packages"
      "PATH=${pkgs.lib.makeBinPath [pkgs.R pythonEnv]}:/bin"
      "R_LIBS_USER=/tmp/R_libs_user"
      "TMPDIR=/tmp/Rtmp"
      "TEMP=/tmp/Rtmp"
      "TMP=/tmp/Rtmp"
      "LC_ALL=C.UTF-8"
      "LANG=C.UTF-8"
    ];
    
    # Use bash as default command and entrypoint script
    Cmd = ["/bin/bash"];
    Entrypoint = [ "${entrypointScript}/bin/entrypoint" ];
    
    # Set up labels
    Labels = {
      "org.opencontainers.image.description" = "Clustbench analysis environment with R and Python";
      "org.opencontainers.image.source" = "https://github.com/username/clustbench-analysis";
    };
  };

  # Write the .Rprofile directly into the image at preparation time
  # and create necessary directories
  extraCommands = ''
    mkdir -p app
    mkdir -p tmp/Rtmp tmp/R_libs_user
    chmod -R 777 tmp
    
    cat > .Rprofile << 'EOF'
${rProfileContent}
EOF
    
    # Also place it in /app so it's found when that directory is mounted
    cp .Rprofile app/.Rprofile
  '';
}