# Plakar UI

Plakar UI is available at the private repository [plakar-ui](https://github.com/plakarkorp/plakar-ui). This repository only contains the compiled build version of the UI.

To synchronize the build assets with the Plakar repository, you can trigger the GitHub Action [Update Plakar UI](https://github.com/brmzkw/plakar/actions/workflows/update-plakar-ui.yml).

**Action Requirements**

The workflow requires the following inputs:
* Plakar UI Git Ref: The Git reference (branch, tag, or commit) of the plakar-ui repository.
* Plakar Branch: The target branch in the plakar repository to apply the changes.

The action will automatically create a pull request in plakar, merging the updates from plakar-ui into the specified destination branch.