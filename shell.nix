# shell.nix
{ pkgs ? import (fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/nixos-24.05.tar.gz";
    sha256 = "0zydsqiaz8qi4zd63zsb2gij2p614cgkcaisnk11wjy3nmiq0x1s";
  }) {} }:

let
  # Import package definitions from default.nix
  defs = import ./nix/default.nix { inherit pkgs; };

  # Extract variables from the imported definitions
  inherit (defs) rPackageList rEnv pythonEnv rProfileContent;

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
    pythonEnv
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
    echo "  python -c 'import pandas as pd; import numpy as np; print(\"Python packages loaded successfully\")'"
    echo ""
  '';
}
