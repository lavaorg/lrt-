# lrt-x
lavaorg runtime-extensions common code appropriate for multiple projects

This is a companion set of common runtime extenstions that may reference externally managed packages. That is packages not
specifically self contained in this package.  During build a set of 'go get' operations will be performed to bring in other 
external packages.

lrt-x will be composed of common packages that make use of or augment external pacakges that are large and under active 
development and thus not suitable for adoption into lrt.

# Licensing
The entire project license is specified in the file LICENSE in the top levl of the repository.

Individual sub-directores or files may override with their own specified license.
