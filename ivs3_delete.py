#!/usr/bin/env python

import argparse

from boto.s3.connection import S3Connection
import invar

import eventlet
eventlet.monkey_patch()


class IVS3_Delete(invar.InvarUtility):
    description = 'Delete a directory of images to S3 concurrently.'

    def _init_common_parser(self):
        """
        Override default parser behavior.
        """
        self.argparser = argparse.ArgumentParser(description=self.description, epilog=self.epilog)

    def add_arguments(self):
        self.argparser.add_argument('bucket',  help='Bucket in which to put files. You may also specify a path component (e.g. a subdirectory to put the files in).', default=None)
        self.argparser.add_argument('bucket_folder', help='Everything below bucket level.', default=None)
        self.argparser.add_argument('-c', '--concurrency', help='Number of concurrent deletes.', type=int, default=32)
        self.argparser.add_argument('-v', '--verbose', action='store_true', help='Display detailed error messages.')

    def main(self):
        if self.args.bucket and self.args.bucket_folder:
            self.conn = S3Connection()
            self.bucket = self.conn.get_bucket(self.args.bucket)
            rs = self.bucket.list(self.args.bucket_folder)

            key_counter = 0
            keys_to_delete = []

            pile = eventlet.GreenPile(self.args.concurrency)

            for key in rs:
                keys_to_delete.append(key.name)
                key_counter += 1

                if key_counter % 1000 == 0:
                    pile.spawn(self.delete, keys_to_delete)
                    keys_to_delete = []

            pile.spawn(self.delete, keys_to_delete)  # Pick up stragglers

            # Wait for all greenlets to finish
            list(pile)

        else:
            print "Can't do it. Not enouch specified. You'll nuke the kernel."

    def delete(self, keys_to_delete):  # Take chunks of up to 1,000 and delete them en masse.
        #print 'Deleting keys beginning with: %s' % keys_to_delete[0]
        #print 'BANG BANG DELETE ......................'
        result = self.bucket.delete_keys(keys_to_delete)
        if len(result.errors) > 0:
            print 'ERROR ERROR ERROR: %s' % result.errors[0]

        print 'Deleted %s keys' % len(keys_to_delete)

if __name__ == "__main__":
    ivs3_delete = IVS3_Delete()
    ivs3_delete.main()
