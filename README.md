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

* The full text of the BSL 1.1 is available in the [LICENSE.md](LICENSE.md) file in this
  repository.
* If your intended use case falls outside the **Additional Use Grant** and you require a
  commercial license, please contact [GerritForge Sales](https://gerritforge.com/contact).

<<<<<<< PATCH SET (b581a8bba10fe4006989f13b4ca586d78eed7c79 Add BSL 1.1 notice in README.md)
=======

>>>>>>> BASE      (be9bbc81706acd56fa7e885833d257875fe6788e Add BSL 1.1 notice in README)
## Access


To be allowed to trigger pull replication a user must be a member of a
group that is granted the 'Pull Replication' capability (provided
by this plugin) or the 'Administrate Server' capability.

## Change Indexing


Changes will be automatically indexed upon replication.


For more information please refer to the [docs](src/main/resources/Documentation)


