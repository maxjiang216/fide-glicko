# Single image for all data-heavy Lambdas. Build once, reuse.
# Avoids 250 MB zip limit and redundant pip installs.
FROM public.ecr.aws/lambda/python:3.12

# Copy application code (DockerContext is repo root)
COPY handlers /var/task/handlers
COPY src /var/task/src
# Scraper modules at root so "from get_player_list import", "from s3_io import" etc. resolve
COPY src/scraper/*.py /var/task/

# Install data deps (pandas, pyarrow, etc.)
COPY docker/requirements.txt /var/task/
RUN pip install -r /var/task/requirements.txt -t /var/task --no-cache-dir \
  --platform manylinux2014_x86_64 --only-binary=:all: \
  && rm -rf /var/task/requirements.txt
