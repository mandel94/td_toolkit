FROM squidfunk/mkdocs-material:latest

# Install additional plugins
RUN pip install --no-cache-dir \
    mkdocs-mermaid2-plugin \
    mkdocs-git-revision-date-localized-plugin \
    mkdocs-minify-plugin \
    pymdown-extensions

# Set working directory
WORKDIR /docs

# Expose documentation port
EXPOSE 8001

# Default command
CMD ["serve", "--dev-addr=0.0.0.0:8001"]
