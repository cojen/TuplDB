This file intended to be used by Tupl developers.

# How to release

Prerequisite: install gpg.

Set JAVA_HOME environment variable to JDK1.8.

Then do:

```
mvn release:clean release:prepare -P release 
```

After this step maven will ask you for the next version, compile sources, build jar, sources jar and javadoc jar.

If something goes wrong this step can easily be reverted by doing ``git reset HEAD --hard`` and then (optionally) ``git clean -f -d`` which will return repository to its initial state.

Once ``release:prepare`` finishes, the artifacts can be published in nexus repository by running

```
mvn release:perform -P release
```

