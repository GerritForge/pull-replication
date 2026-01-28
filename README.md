# Gerrit pull-replication plugin

This plugin can automatically mirror repositories from other systems.

Overview
--------

Typically replication should be done over SSH, with a passwordless
public/private key pair. On a trusted network it is also possible to
use replication over the insecure (but much faster due to no
authentication overhead or encryption) git:// protocol, by enabling
the `upload-pack` service on the receiving system, but this
configuration is not recommended. It is also possible to specify a
local path as replication source. This makes sense if a network
share is mounted to which the repositories should be replicated from.

## License

This project is licensed under the **Business Source License 1.1** (BSL 1.1).
This is a "source-available" license that balances free, open-source-style access to the code
with temporary commercial restrictions.

* The full text of the BSL 1.1 is available in the [LICENSE](LICENSE) file in this
  repository.
* If your intended use case falls outside the **Additional Use Grant** and you require a
  commercial license, please contact [GerritForge Sales](https://gerritforge.com/contact).


## Access


To be allowed to trigger pull replication a user must be a member of a
group that is granted the 'Pull Replication' capability (provided
by this plugin) or the 'Administrate Server' capability.

## Change Indexing


Changes will be automatically indexed upon replication.


For more information please refer to the [docs](src/main/resources/Documentation)


