FROM centos:7

# This Dockerfile creates an image with oracle client and data_check

RUN yum install -y wget  ncurses libnsl && \
    yum-config-manager --add-repo https://yum.oracle.com/public-yum-ol7.repo && \
    wget http://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle && \
    yum install -y oracle-release-el7 && \
    mv /etc/yum.repos.d/oracle-ol7.repo.incomplete /etc/yum.repos.d/oracle-ol7.repo && \
    yum install -y oracle-instantclient19.5-basic oracle-instantclient19.5-sqlplus python3 && \
    yum clean all && rm -rf /var/cache/yum && rm /etc/yum.repos.d/oracle* && rm /etc/yum.repos.d/public-yum-ol7.repo

ENV CLIENT_HOME=/usr/lib/oracle/19.5/client64
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${CLIENT_HOME}/lib"
ENV PATH="${PATH}:${CLIENT_HOME}/bin"
ENV NLS_LANG=".utf8"
ENV LC_ALL=en_US.utf-8
ENV LANG=en_US.utf-8

RUN useradd data_check && mkdir /app && chown data_check /app
USER data_check

WORKDIR /app
ENV PATH="/home/data_check/.local/bin/:${PATH}"

RUN python3 -m pip install --user poetry pytest

COPY --chown=data_check . .
COPY --chown=data_check int_test/scripts .

RUN poetry build && python3 -m pip install --user ".[oracle]"

CMD ["sh", "-c", "/app/run_tests.sh oracle"]
