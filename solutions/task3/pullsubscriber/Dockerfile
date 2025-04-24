FROM google/cloud-sdk:519.0.0

WORKDIR /workspace/

COPY . ./

RUN python3 -m pip install \
    --requirement requirements.txt \
    --find-links vendor \
    --break-system-packages

CMD ["python3", "main.py"]