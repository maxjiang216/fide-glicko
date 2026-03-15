# Single image for all data-heavy Lambdas. Build once, reuse.
# Avoids 250 MB zip limit and redundant pip installs.
FROM public.ecr.aws/lambda/python:3.12

# Copy application code (DockerContext is repo root)
COPY handlers /var/task/handlers
COPY src /var/task/src

# Install data deps (pandas, pyarrow, etc.)
COPY docker/requirements.txt /var/task/
RUN pip install -r /var/task/requirements.txt -t /var/task --no-cache-dir \
  --platform manylinux2014_x86_64 --only-binary=:all: \
  && rm -rf /var/task/requirements.txt
