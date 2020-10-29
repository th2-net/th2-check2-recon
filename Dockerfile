FROM python:3.8-slim
ARG pypi_repository_url
ARG pypi_user
ARG pypi_password
ARG app_name
ARG app_version
WORKDIR /usr/src/app
COPY . .
RUN printf '{"package_name":"%s","package_version":"%s"}' "$app_name" "$app_version" > "package_info.json" && \
    pip install -r requirements.txt && \
    python setup.py sdist && \
    twine upload --repository-url ${pypi_repository_url} --username ${pypi_user} --password ${pypi_password} dist/*