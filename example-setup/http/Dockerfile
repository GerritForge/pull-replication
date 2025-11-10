FROM gerritcodereview/gerrit:3.6.8-almalinux8

USER root

RUN rpm --import https://repo.almalinux.org/almalinux/RPM-GPG-KEY-AlmaLinux
RUN dnf clean all
RUN dnf update -y almalinux-release
RUN dnf install -y gettext

RUN git config -f /var/gerrit/etc/secure.config --add auth.bearerToken "theSecretBearerToken"

COPY --chown=gerrit:gerrit pull-replication.jar /var/gerrit/plugins/pull-replication.jar
COPY --chown=gerrit:gerrit entrypoint.sh /tmp/
COPY --chown=gerrit:gerrit configs/replication.config.template /var/gerrit/etc/
COPY --chown=gerrit:gerrit configs/gerrit.config.template /var/gerrit/etc/

ENTRYPOINT [ "/tmp/entrypoint.sh" ]
