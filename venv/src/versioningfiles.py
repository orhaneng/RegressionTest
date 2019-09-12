'''
https://www.oreilly.com/library/view/python-cookbook/0596001673/ch04s26.html
'''


def VersionFile(file_source, file_spec):
    import os, shutil

    for root, dirs, files in os.walk(batch_file_dir):

        if os.path.isfile(file_spec):
            # Determine root filename so the extension doesn't get longer
            n, e = os.path.splitext(file_spec)

            # Is e an integer?
            try:
                num = int(e)
                root = n
            except ValueError:
                root = file_spec
            # Find next available file version
            for i in range(10000):
                new_file = '%s.%03d' % (root, i)
                shutil.move(file_source, new_file)
                return new_file
        else:
            shutil.move(file_source, file_spec)
    return 0


if __name__ == '__main__':
    # test code (you will need a file named test.txt)
    print(VersionFile('/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/versiontest/',
                      '/Users/omerorhan/Documents/EventDetection/regression_server/regressiontest/versiontest/backup/test.txt'))
