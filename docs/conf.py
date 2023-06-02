# -*- coding: utf-8 -*-
import os
import sys
from datetime import date

from sphinx_scylladb_theme.utils import multiversion_regex_builder

sys.path.insert(0, os.path.abspath('..'))
import cassandra

# -- General configuration -----------------------------------------------------

# Build documentation for the following tags and branches
TAGS = ['3.21.0-scylla', '3.22.3-scylla', '3.24.8-scylla', '3.25.4-scylla', '3.25.11-scylla', '3.26.2-scylla']
BRANCHES = ['master']
# Set the latest version.
LATEST_VERSION = '3.26.2-scylla'
# Set which versions are not released yet.
UNSTABLE_VERSIONS = ['master']
# Set which versions are deprecated
DEPRECATED_VERSIONS = ['']

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.todo',
    'sphinx.ext.mathjax',
    'sphinx.ext.githubpages',
    'sphinx.ext.extlinks',
    'sphinx_sitemap',
    'sphinx_scylladb_theme',
    'sphinx_multiversion',  # optional
    'recommonmark',  # optional
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix(es) of source filenames.
source_suffix = [".rst", ".md"]

# The encoding of source files.
#source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = u'Scylla Python Driver'
copyright = u'ScyllaDB 2021 and © DataStax 2013-2017'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = cassandra.__version__
# The full version, including alpha/beta/rc tags.
release = cassandra.__version__

autodoc_member_order = 'bysource'
autoclass_content = 'both'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['_build', 'cloud.rst', 'core_graph.rst', 'classic_graph.rst', 'geo_types.rst', 'graph.rst', 'graph_fluent.rst']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# -- Options for not found extension -------------------------------------------

# Template used to render the 404.html generated by this extension.
notfound_template =  '404.html'

# Prefix added to all the URLs generated in the 404 page.
notfound_urls_prefix = ''

# -- Options for multiversion --------------------------------------------------

# Whitelist pattern for tags
smv_tag_whitelist = multiversion_regex_builder(TAGS)
# Whitelist pattern for branches
smv_branch_whitelist = multiversion_regex_builder(BRANCHES)
# Defines which version is considered to be the latest stable version.
smv_latest_version = LATEST_VERSION
# Defines the new name for the latest version.
smv_rename_latest_version = 'stable'
# Whitelist pattern for remotes (set to None to use local branches only)
smv_remote_whitelist = r'^origin$'
# Pattern for released versions
smv_released_pattern = r'^tags/.*$'
# Format for versioned output directories inside the build directory
smv_outputdir_format = '{ref.name}'

# -- Options for HTML output --------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'sphinx_scylladb_theme'

# -- Options for sitemap extension ---------------------------------------

sitemap_url_scheme = "/stable/{link}"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    'conf_py_path': 'docs/',
    'github_repository': 'scylladb/python-driver',
    'github_issues_repository': 'scylladb/python-driver',
    'hide_edit_this_page_button': 'false',
    'hide_version_dropdown': ['master'],
    'hide_feedback_buttons': 'false',
    'versions_unstable': UNSTABLE_VERSIONS,
    'versions_deprecated': DEPRECATED_VERSIONS,
}

# Custom sidebar templates, maps document names to template names.
html_sidebars = {'**': ['side-nav.html']}

# If false, no index is generated.
html_use_index = False

# Output file base name for HTML help builder.
htmlhelp_basename = 'CassandraDriverdoc'

# URL which points to the root of the HTML documentation. 
html_baseurl = 'https://python-driver.docs.scylladb.com'

# Dictionary of values to pass into the template engine’s context for all pages
html_context = {'html_baseurl': html_baseurl}

