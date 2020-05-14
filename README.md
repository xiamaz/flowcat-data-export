# Anonymize fcs files

Remove metadata from FCS headers through direct binary editing to avoid loving
other relevant metadata.

The anonymizer script only blanks the respective fields using spaces and blanks
device specific data, such as acquisition protocol saved by Navious through
blanking. This approach is strictly inferior to
[fcscleaner](https://github.com/xiamaz/fcscleaner), which parses the header
correctly and only saves the relevant text and data segments into a new file,
resulting in smaller FCS files.

This code was used for exporting the test dataset for uploading to
<https://flowrepository.org>.
