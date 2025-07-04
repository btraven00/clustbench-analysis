# shell.nix
{ pkgs ? import (fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/refs/tags/25.11-pre.tar.gz";
    sha256 = "1r4fhp3apf1qggsrm60ni820gxzpm04q9xdk1w3dap9qymi6bpdk";
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
    pkgs.texlive.combined.scheme-full  # Full TeX Live distribution
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
    echo "LaTeX installation available via texlive"
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
