
# Publishing

This is a composer library and is published to our private package repo at
https://packages.bunnysites.com/

This is IP restricted to our office and website servers. There's also a super-secret password for those working at home.

To publish, follow these steps:

```sh
# tag it
git tag -a v1.2.3 -m "Something important."

# push to master
git push origin master

# publish it (this is a BSTS hook)
composer publish
```

Note, the publish script will only work on the office IP.


To update your project:

```sh
# For a new feature or major version
composer require karmabunny/rmsapi:^1.2

# For whatever existing constraint
composer update karmabunny/rmsapi
```


## Versioning

The version tag works like this: `breaking.feature.patch`

Increment the...

1. first component if we've broken something backwards compatible
2. second component for new features
3. third component for patch fixes


## Constraints

When adding this as a dependency, you'll specify not a specific version but instead a fuzzy 'constraint' version. E.g. `'^1.2'` or `'^1'`.

This allows `composer update` to get the latest patch or feature components of a package without you having to know or specify the explicit version.