FROM public.ecr.aws/bitnami/python:3.10

# Create a system group named "localuser"
RUN addgroup --system localuser

# Create a system user named "localuser" and assign them to the "localuser" group
RUN adduser --system --ingroup localuser localuser

# Create the /home/localuser directory and set permissions
RUN mkdir -p /home/localuser && chown -R localuser:localuser /home/localuser

# Set the user to "localuser" for subsequent commands
USER localuser

# Set the HOME environment variable
ENV HOME /home/localuser

# Add the user's bin directory to the PATH
ENV PATH="${HOME}/.local/bin:${PATH}"

WORKDIR /montblanc_veeva_importer

COPY src/ /montblanc_veeva_importer
COPY montblanc_utils/ /montblanc_veeva_importer/montblanc_utils
COPY common_utils/ /montblanc_veeva_importer/common_utils
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENTRYPOINT [ "python" ]
CMD [ "/montblanc_veeva_importer/load_mb_objects_to_gemstone.py"]