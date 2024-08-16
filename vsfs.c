/*
 * This code is provided solely for the personal and private use of students
 * taking the CSC369H course at the University of Toronto. Copying for purposes
 * other than this use is expressly prohibited. All forms of distribution of
 * this code, including but not limited to public repositories on GitHub,
 * GitLab, Bitbucket, or any other online platform, whether as given or with
 * any changes, are expressly prohibited.
 *
 * Authors: Alexey Khrabrov, Karen Reid, Angela Demke Brown
 *
 * All of the files in this directory and all subdirectories are:
 * Copyright (c) 2022 Angela Demke Brown
 */

/**
 * CSC369 Assignment 4 - vsfs driver implementation.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

// Using 2.9.x FUSE API
#define FUSE_USE_VERSION 29
#include <fuse.h>

#include "vsfs.h"
#include "fs_ctx.h"
#include "options.h"
#include "util.h"
#include "bitmap.h"
#include "map.h"

// NOTE: All path arguments are absolute paths within the vsfs file system and
//  start with a '/' that corresponds to the vsfs root directory.
//
//  For example, if vsfs is mounted at "/tmp/my_userid", the path to a
//  file at "/tmp/my_userid/dir/file" (as seen by the OS) will be
//  passed to FUSE callbacks as "/dir/file".
//
//  Paths to directories (except for the root directory - "/") do not end in a
//  trailing '/'. For example, "/tmp/my_userid/dir/" will be passed to
//  FUSE callbacks as "/dir".

/**
 * Initialize the file system.
 *
 * Called when the file system is mounted. NOTE: we are not using the FUSE
 * init() callback since it doesn't support returning errors. This function must
 * be called explicitly before fuse_main().
 *
 * @param fs    file system context to initialize.
 * @param opts  command line options.
 * @return      true on success; false on failure.
 */
static bool vsfs_init(fs_ctx *fs, vsfs_opts *opts)
{
	size_t size;
	void *image;

	// Nothing to initialize if only printing help
	if (opts->help)
	{
		return true;
	}

	// Map the disk image file into memory
	image = map_file(opts->img_path, VSFS_BLOCK_SIZE, &size);
	if (image == NULL)
	{
		return false;
	}

	return fs_ctx_init(fs, image, size);
}

/**
 * Cleanup the file system.
 *
 * Called when the file system is unmounted. Must cleanup all the resources
 * created in vsfs_init().
 */
static void vsfs_destroy(void *ctx)
{
	fs_ctx *fs = (fs_ctx *)ctx;
	if (fs->image)
	{
		munmap(fs->image, fs->size);
		fs_ctx_destroy(fs);
	}
}

/** Get file system context. */
static fs_ctx *get_fs(void)
{
	return (fs_ctx *)fuse_get_context()->private_data;
}

// HELPER Function + Constants
#define NUM_ENTRIES_PER_BLK (int)(VSFS_BLOCK_SIZE / sizeof(vsfs_dentry))
#define NUM_BLKS_PER_INDIRECT_BLK (int)(VSFS_BLOCK_SIZE / sizeof(vsfs_blk_t))
// Return the pointer of inode given inode number ino
vsfs_inode *get_inode(vsfs_ino_t ino)
{
	fs_ctx *fs = get_fs();
	return &(fs->itable[ino]);
}

// Return the pointer of dentry given block number blk
vsfs_dentry *get_dentry(vsfs_blk_t blk)
{
	fs_ctx *fs = get_fs();
	return (vsfs_dentry *)(fs->image + blk * VSFS_BLOCK_SIZE);
}

/* Returns the inode number for the element at the end of the path
 * if it exists.  If there is any error, return -1.
 * Possible errors include:
 *   - The path is not an absolute path
 *   - An element on the path cannot be found
 */
static int path_lookup(const char *path, vsfs_ino_t *ino, vsfs_dentry **dentry_ptr)
{
	if (path[0] != '/')
	{
		fprintf(stderr, "Not an absolute path\n");
		return -1;
	}

	// TODO: complete this function and any helper functions
	if (strcmp(path, "/") == 0)
	{
		*ino = VSFS_ROOT_INO;
		return 0;
	}

	// get the inode of the root directory
	vsfs_inode *root = get_inode(VSFS_ROOT_INO);

	vsfs_dentry *dentry;

	// Loop through all data blocks of the root directory to find the element
	for (vsfs_blk_t i = 0; i < root->i_blocks; i++)
	{
		if (i < VSFS_NUM_DIRECT)
		{
			// data block is in the direct data blocks
			dentry = get_dentry(root->i_direct[i]);

			for (int j = 0; j < NUM_ENTRIES_PER_BLK; j++)
			{
				if (strcmp(dentry[j].name, path + 1) == 0)
				{
					*ino = dentry[j].ino;
					if (dentry_ptr)
					{
						*dentry_ptr = dentry + j;
					}
					return 0;
				}
			}
		}
		else
		{
			// Indirect data pointers
			vsfs_blk_t *indirect_data = (vsfs_blk_t *)(get_fs()->image + root->i_indirect * VSFS_BLOCK_SIZE);
			dentry = get_dentry(indirect_data[i - VSFS_NUM_DIRECT]);

			for (int j = 0; j < NUM_ENTRIES_PER_BLK; j++)
			{
				if (strcmp(dentry[j].name, path + 1) == 0)
				{
					*ino = dentry[j].ino;
					if (dentry_ptr)
					{
						*dentry_ptr = dentry + j;
					}
					return 0;
				}
			}
		}
	}

	return -1;
}

/**
 * Get file system statistics.
 *
 * Implements the statvfs() system call. See "man 2 statvfs" for details.
 * The f_bfree and f_bavail fields should be set to the same value.
 * The f_ffree and f_favail fields should be set to the same value.
 * The following fields can be ignored: f_fsid, f_flag.
 * All remaining fields are required.
 *
 * Errors: none
 *
 * @param path  path to any file in the file system. Can be ignored.
 * @param st    pointer to the struct statvfs that receives the result.
 * @return      0 on success; -errno on error.
 */
static int vsfs_statfs(const char *path, struct statvfs *st)
{
	(void)path; // unused
	fs_ctx *fs = get_fs();
	vsfs_superblock *sb = fs->sb; /* Get ptr to superblock from context */

	memset(st, 0, sizeof(*st));
	st->f_bsize = VSFS_BLOCK_SIZE;	   /* Filesystem block size */
	st->f_frsize = VSFS_BLOCK_SIZE;	   /* Fragment size */
									   // The rest of required fields are filled based on the information
									   // stored in the superblock.
	st->f_blocks = sb->sb_num_blocks;  /* Size of fs in f_frsize units */
	st->f_bfree = sb->sb_free_blocks;  /* Number of free blocks */
	st->f_bavail = sb->sb_free_blocks; /* Free blocks for unpriv users */
	st->f_files = sb->sb_num_inodes;   /* Number of inodes */
	st->f_ffree = sb->sb_free_inodes;  /* Number of free inodes */
	st->f_favail = sb->sb_free_inodes; /* Free inodes for unpriv users */

	st->f_namemax = VSFS_NAME_MAX; /* Maximum filename length */

	return 0;
}

/**
 * Get file or directory attributes.
 *
 * Implements the lstat() system call. See "man 2 lstat" for details.
 * The following fields can be ignored: st_dev, st_ino, st_uid, st_gid, st_rdev,
 *                                      st_blksize, st_atim, st_ctim.
 * All remaining fields are required.
 *
 * NOTE: the st_blocks field is measured in 512-byte units (disk sectors);
 *       it should include any metadata blocks that are allocated to the
 *       inode (for vsfs, that is the indirect block).
 *
 * NOTE2: the st_mode field must be set correctly for files and directories.
 *
 * Errors:
 *   ENAMETOOLONG  the path or one of its components is too long.
 *   ENOENT        a component of the path does not exist.
 *   ENOTDIR       a component of the path prefix is not a directory.
 *
 * @param path  path to a file or directory.
 * @param st    pointer to the struct stat that receives the result.
 * @return      0 on success; -errno on error;
 */
static int vsfs_getattr(const char *path, struct stat *st)
{
	if (strlen(path) >= VSFS_PATH_MAX)
		return -ENAMETOOLONG;
	fs_ctx *fs = get_fs();

	memset(st, 0, sizeof(*st));

	// TODO: lookup the inode for given path and, if it exists, fill in the
	//  required fields based on the information stored in the inode
	(void)fs;
	(void)path_lookup;

	vsfs_ino_t ino;
	if (path_lookup(path, &ino, NULL) == 0)
	{
		vsfs_inode *inode = get_inode(ino);
		st->st_ino = ino;
		st->st_mode = inode->i_mode;
		st->st_nlink = inode->i_nlink;
		st->st_size = inode->i_size;
		st->st_blocks = inode->i_blocks * VSFS_BLOCK_SIZE / 512;
		st->st_mtim = inode->i_mtime;

		// Include an additional block allocated for indirect block
		if (inode->i_blocks > VSFS_NUM_DIRECT)
		{
			st->st_blocks += VSFS_BLOCK_SIZE / 512;
		}

		return 0;
	}

	return -ENOENT;
}

/**
 * Read a directory.
 *
 * Implements the readdir() system call. Should call filler(buf, name, NULL, 0)
 * for each directory entry. See fuse.h in libfuse source code for details.
 *
 * Assumptions (already verified by FUSE using getattr() calls):
 *   "path" exists and is a directory.
 *
 * Errors:
 *   ENOMEM  not enough memory (e.g. a filler() call failed).
 *
 * @param path    path to the directory.
 * @param buf     buffer that receives the result.
 * @param filler  function that needs to be called for each directory entry.
 *                Pass 0 as offset (4th argument). 3rd argument can be NULL.
 * @param offset  unused.
 * @param fi      unused.
 * @return        0 on success; -errno on error.
 */
static int vsfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
						off_t offset, struct fuse_file_info *fi)
{
	(void)offset; // unused
	(void)fi;	  // unused
	fs_ctx *fs = get_fs();

	// TODO: lookup the directory inode for the given path and iterate
	//       through its directory entries
	(void)fs;
	vsfs_ino_t ino;
	if (path_lookup(path, &ino, NULL) == 0)
	{
		// By assumption, we know that the inode is a directory
		assert(ino == VSFS_ROOT_INO); // ino refers to the root
		vsfs_inode *inode = get_inode(ino);
		vsfs_blk_t *indirect_data = (vsfs_blk_t *)(get_fs()->image + inode->i_indirect * VSFS_BLOCK_SIZE);

		for (vsfs_blk_t i = 0; i < inode->i_blocks; i++)
		{
			if (i < VSFS_NUM_DIRECT)
			{
				// Direct data blocks
				vsfs_dentry *dentry = get_dentry(inode->i_direct[i]);
				for (int j = 0; j < NUM_ENTRIES_PER_BLK; j++)
				{
					if (dentry[j].ino != VSFS_INO_MAX)
					{
						if (filler(buf, dentry[j].name, NULL, 0) != 0)
						{
							return -ENOMEM;
						}
					}
				}
			}
			else
			{
				// Indirect data pointers
				vsfs_dentry *dentry = get_dentry(indirect_data[i - VSFS_NUM_DIRECT]);
				for (int j = 0; j < NUM_ENTRIES_PER_BLK; j++)
				{
					if (dentry[j].ino != VSFS_INO_MAX)
					{
						if (filler(buf, dentry[j].name, NULL, 0) != 0)
						{
							return -ENOMEM;
						}
					}
				}
			}
		}
	}

	return 0;
}

/**
 * Create a file.
 *
 * Implements the open()/creat() system call.
 *
 * Assumptions (already verified by FUSE using getattr() calls):
 *   "path" doesn't exist.
 *   The parent directory of "path" exists and is a directory.
 *   "path" and its components are not too long.
 *
 * Errors:
 *   ENOMEM  not enough memory (e.g. a malloc() call failed).
 *   ENOSPC  not enough free space in the file system.
 *
 * @param path  path to the file to create.
 * @param mode  file mode bits.
 * @param fi    unused.
 * @return      0 on success; -errno on error.
 */
static int vsfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	(void)fi; // unused
	assert(S_ISREG(mode));
	fs_ctx *fs = get_fs();

	// Done: create a file at given path with given mode
	(void)path;
	// Check if there is enough space in the file system
	if (fs->sb->sb_free_inodes == 0)
	{
		return -ENOSPC;
	}
	// find a free inode
	u_int32_t inode_index;
	vsfs_blk_t nblks = fs->size / VSFS_BLOCK_SIZE;
	if (bitmap_alloc(fs->ibmap, nblks, &inode_index) != 0)
	{
		return -ENOSPC;
	}

	// find an empty dentry
	vsfs_inode *root_inode = get_inode(VSFS_ROOT_INO);
	vsfs_dentry *new_dentry;
	// loop through all blocks in the root inode to find an empty dentry
	for (vsfs_blk_t i = 0; i < root_inode->i_blocks; i++)
	{
		if (i < VSFS_NUM_DIRECT)
		{
			// Direct data blocks
			new_dentry = get_dentry(root_inode->i_direct[i]);
			for (int j = 0; j < NUM_ENTRIES_PER_BLK; j++)
			{
				if (new_dentry[j].ino == VSFS_INO_MAX)
				{
					// Found an empty dentry
					new_dentry[j].ino = inode_index;
					strncpy(new_dentry[j].name, path + 1, VSFS_NAME_MAX);
					// mark the inode as used, and decrement the number of free inodes
					bitmap_set(fs->ibmap, nblks, inode_index, true);
					fs->sb->sb_free_inodes--;
					// find the inode given the index and update the fields
					vsfs_inode *new_inode = get_inode(inode_index);
					memset(new_inode, 0, sizeof(vsfs_inode));
					new_inode->i_mode = mode;
					new_inode->i_nlink = 1;
					new_inode->i_size = 0;
					new_inode->i_blocks = 0;
					if (clock_gettime(CLOCK_REALTIME, &(new_inode->i_mtime)) != 0)
					{
						perror("clock_gettime");
						return -1;
					}
					if (clock_gettime(CLOCK_REALTIME, &(root_inode->i_mtime)) != 0)
					{
						perror("clock_gettime");
						return -1;
					}

					return 0;
				}
			}
		}
		else
		{
			// Indirect data pointers
			vsfs_blk_t *indirect_data = (vsfs_blk_t *)(fs->image + root_inode->i_indirect * VSFS_BLOCK_SIZE);
			new_dentry = get_dentry(indirect_data[i - VSFS_NUM_DIRECT]);
			for (int j = 0; j < NUM_ENTRIES_PER_BLK; j++)
			{
				if (new_dentry[j].ino == VSFS_INO_MAX)
				{
					// Found an empty dentry
					new_dentry[j].ino = inode_index;
					strncpy(new_dentry[j].name, path + 1, VSFS_NAME_MAX);
					// mark the inode as used, and decrement the number of free inodes
					bitmap_set(fs->ibmap, nblks, inode_index, true);
					fs->sb->sb_free_inodes--;
					// find the inode given the index and update the fields
					vsfs_inode *new_inode = get_inode(inode_index);
					memset(new_inode, 0, sizeof(vsfs_inode));
					new_inode->i_mode = mode;
					new_inode->i_nlink = 1;
					new_inode->i_size = 0;
					new_inode->i_blocks = 0;
					if (clock_gettime(CLOCK_REALTIME, &(new_inode->i_mtime)) != 0)
					{
						perror("clock_gettime");
						return -1;
					}
					if (clock_gettime(CLOCK_REALTIME, &(root_inode->i_mtime)) != 0)
					{
						perror("clock_gettime");
						return -1;
					}

					return 0;
				}
			}
		}
	}

	// If we get here, that means there is no empty dentry in the current blocks
	// Need to allocate a new block if there is still space
	vsfs_blk_t new_block_number;
	if (root_inode->i_blocks >= (VSFS_NUM_DIRECT + NUM_BLKS_PER_INDIRECT_BLK))
	{
		// all entries are filled
		return -ENOSPC;
	}
	// allocate a new block
	if (bitmap_alloc(fs->dbmap, nblks, &(new_block_number)) != 0)
	{
		return -ENOSPC;
	}
	if (root_inode->i_blocks < VSFS_NUM_DIRECT)
	{
		root_inode->i_direct[root_inode->i_blocks] = new_block_number;
		new_dentry = get_dentry(new_block_number);
		new_dentry[0].ino = inode_index;
		strncpy(new_dentry[0].name, path + 1, VSFS_NAME_MAX);
		// update the root inode and superblock
		root_inode->i_blocks++;
		root_inode->i_size += VSFS_BLOCK_SIZE;
		fs->sb->sb_free_blocks--;
		// mark the block as used
		bitmap_set(fs->dbmap, nblks, new_block_number, true);
	}
	else if (root_inode->i_blocks == VSFS_NUM_DIRECT)
	{
		// allocate an indirect block
		vsfs_blk_t indirect_block_number;
		if (bitmap_alloc(fs->dbmap, nblks, &(indirect_block_number)) != 0)
		{
			return -ENOSPC;
		}
		memset(fs->image + indirect_block_number * VSFS_BLOCK_SIZE, 0, VSFS_BLOCK_SIZE);
		// write the indirect block to the disk
		root_inode->i_indirect = indirect_block_number;
		vsfs_blk_t *indirect_data = (vsfs_blk_t *)(fs->image + root_inode->i_indirect * VSFS_BLOCK_SIZE);
		indirect_data[0] = new_block_number;

		// write the dentry to disk
		new_dentry = get_dentry(new_block_number);
		new_dentry[0].ino = inode_index;
		strncpy(new_dentry[0].name, path + 1, VSFS_NAME_MAX);

		// update the root inode and superblock
		root_inode->i_blocks++;
		root_inode->i_size += VSFS_BLOCK_SIZE;
		fs->sb->sb_free_blocks -= 2;
		// mark the block as used
		bitmap_set(fs->dbmap, nblks, indirect_block_number, true);
		bitmap_set(fs->dbmap, nblks, new_block_number, true);
	}
	else if (root_inode->i_blocks < (VSFS_NUM_DIRECT + NUM_BLKS_PER_INDIRECT_BLK))
	{
		// root->i_blocks in between (VSFS_NUM_DIRECT, VSFS_NUM_DIRECT + NUM_BLKS_PER_INDIRECT_BLK)
		// find the indirect block
		vsfs_blk_t *indirect_data = (vsfs_blk_t *)(fs->image + root_inode->i_indirect * VSFS_BLOCK_SIZE);
		// find and update the indirect block
		indirect_data[root_inode->i_blocks - VSFS_NUM_DIRECT] = new_block_number;
		// write the dentry to disk
		new_dentry = get_dentry(new_block_number);
		new_dentry[0].ino = inode_index;
		strncpy(new_dentry[0].name, path + 1, VSFS_NAME_MAX);

		// update the root inode and superblock
		root_inode->i_blocks++;
		root_inode->i_size += VSFS_BLOCK_SIZE;
		fs->sb->sb_free_blocks--;

		// mark the block as used
		bitmap_set(fs->dbmap, nblks, new_block_number, true);
	}
	// Set the .ino field to VSFS_INO_MAX for directory entries in a block that are not in use
	for (int i = 1; i < NUM_ENTRIES_PER_BLK; i++)
	{
		new_dentry[i].ino = VSFS_INO_MAX;
	}

	bitmap_set(fs->ibmap, nblks, inode_index, true);
	fs->sb->sb_free_inodes--;
	// find the inode given the index and update the fields
	vsfs_inode *new_inode = get_inode(inode_index);
	memset(new_inode, 0, sizeof(vsfs_inode));
	new_inode->i_mode = mode;
	new_inode->i_nlink = 1;
	new_inode->i_size = 0;
	new_inode->i_blocks = 0;
	if (clock_gettime(CLOCK_REALTIME, &(new_inode->i_mtime)) != 0)
	{
		perror("clock_gettime");
		return -1;
	}
	if (clock_gettime(CLOCK_REALTIME, &(root_inode->i_mtime)) != 0)
	{
		perror("clock_gettime");
		return -1;
	}

	return 0;
}

/**
 * Remove a file.
 *
 * Implements the unlink() system call.
 *
 * Assumptions (already verified by FUSE using getattr() calls):
 *   "path" exists and is a file.
 *
 * Errors: none
 *
 * @param path  path to the file to remove.
 * @return      0 on success; -errno on error.
 */
static int vsfs_unlink(const char *path)
{
	fs_ctx *fs = get_fs();
	vsfs_blk_t nblks = fs->size / VSFS_BLOCK_SIZE;

	// TODO: remove the file at given path
	vsfs_ino_t ino;
	vsfs_dentry *dentry;
	int ret = path_lookup(path, &ino, &dentry);
	// By assumption, path is valid and ret should be 0
	assert(ret == 0);

	// clear dentry
	dentry->ino = VSFS_INO_MAX;
	strncpy(dentry->name, "", VSFS_NAME_MAX);
	// clear inode
	vsfs_inode *inode = get_inode(ino);
	for (vsfs_blk_t i = 0; i < inode->i_blocks; i++)
	{
		if (i < VSFS_NUM_DIRECT)
		{
			// Remove Direct data blocks
			vsfs_blk_t *block = (vsfs_blk_t *)(fs->image + inode->i_direct[i] * VSFS_BLOCK_SIZE);
			memset(block, 0, VSFS_BLOCK_SIZE);
			bitmap_free(fs->dbmap, nblks, inode->i_direct[i]);
			fs->sb->sb_free_blocks++;
		}
		else
		{
			// Remove Indirect data block
			vsfs_blk_t *indirect_data = (vsfs_blk_t *)(fs->image + inode->i_indirect * VSFS_BLOCK_SIZE);
			vsfs_blk_t *block = (vsfs_blk_t *)(fs->image + indirect_data[i - VSFS_NUM_DIRECT] * VSFS_BLOCK_SIZE);
			memset(block, 0, VSFS_BLOCK_SIZE);
			bitmap_free(fs->dbmap, nblks, indirect_data[i - VSFS_NUM_DIRECT]);
			fs->sb->sb_free_blocks++;
		}
	}
	if (inode->i_blocks > VSFS_NUM_DIRECT)
	{
		// Remove indirect block
		vsfs_blk_t *indirect_data = (vsfs_blk_t *)(fs->image + inode->i_indirect * VSFS_BLOCK_SIZE);
		memset(indirect_data, 0, VSFS_BLOCK_SIZE);
		bitmap_free(fs->dbmap, nblks, inode->i_indirect);
		fs->sb->sb_free_blocks++;
	}
	bitmap_free(fs->ibmap, nblks, ino);
	fs->sb->sb_free_inodes++;
	memset(inode, 0, sizeof(vsfs_inode));

	// update the modified time of root inode
	vsfs_inode *root_inode = get_inode(VSFS_ROOT_INO);
	if (clock_gettime(CLOCK_REALTIME, &(root_inode->i_mtime)) != 0)
	{
		perror("clock_gettime");
		return -1;
	}

	return 0;
}

/**
 * Change the modification time of a file or directory.
 *
 * Implements the utimensat() system call. See "man 2 utimensat" for details.
 *
 * NOTE: You only need to implement the setting of modification time (mtime).
 *       Timestamp modifications are not recursive.
 *
 * Assumptions (already verified by FUSE using getattr() calls):
 *   "path" exists.
 *
 * Errors: none
 *
 * @param path   path to the file or directory.
 * @param times  timestamps array. See "man 2 utimensat" for details.
 * @return       0 on success; -errno on failure.
 */
static int vsfs_utimens(const char *path, const struct timespec times[2])
{
	fs_ctx *fs = get_fs();
	vsfs_inode *ino = NULL;

	// Done: update the modification timestamp (mtime) in the inode for given
	//  path with either the time passed as argument or the current time,
	//  according to the utimensat man page
	(void)fs;
	// 0. Check if there is actually anything to be done.
	if (times[1].tv_nsec == UTIME_OMIT)
	{
		// Nothing to do.
		return 0;
	}

	// 1. TODO: Find the inode for the final component in path
	vsfs_ino_t inode_index;
	int ret = path_lookup(path, &inode_index, NULL);
	assert(ret == 0);
	ino = get_inode(inode_index);

	// 2. Update the mtime for that inode.
	//    This code is commented out to avoid failure until you have set
	//    'ino' to point to the inode structure for the inode to update.
	if (times[1].tv_nsec == UTIME_NOW)
	{
		if (clock_gettime(CLOCK_REALTIME, &(ino->i_mtime)) != 0)
		{
			// clock_gettime should not fail, unless you give it a
			// bad pointer to a timespec.
			assert(false);
		}
	}
	else
	{
		ino->i_mtime = times[1];
	}

	return 0;
}

/**
 * Change the size of a file.
 *
 * Implements the truncate() system call. Supports both extending and shrinking.
 * If the file is extended, the new uninitialized range at the end must be
 * filled with zeros.
 *
 * Assumptions (already verified by FUSE using getattr() calls):
 *   "path" exists and is a file.
 *
 * Errors:
 *   ENOMEM  not enough memory (e.g. a malloc() call failed).
 *   ENOSPC  not enough free space in the file system.
 *   EFBIG   write would exceed the maximum file size.
 *
 * @param path  path to the file to set the size.
 * @param size  new file size in bytes.
 * @return      0 on success; -errno on error.
 */
static int vsfs_truncate(const char *path, off_t size)
{
	fs_ctx *fs = get_fs();
	(void)fs;
	// TODO: set new file size, possibly "zeroing out" the uninitialized range

	// check if the new size is larger than the maximum file size
	if (size > (off_t)(NUM_BLKS_PER_INDIRECT_BLK + VSFS_NUM_DIRECT) * VSFS_BLOCK_SIZE)
	{
		return -EFBIG;
	}

	vsfs_blk_t nblks = fs->size / VSFS_BLOCK_SIZE;
	// locate the inode
	vsfs_ino_t inode_index;
	int ret = path_lookup(path, &inode_index, NULL);
	assert(ret == 0);
	vsfs_inode *inode = get_inode(inode_index);
	// check if the size is the same as the old size
	if (size == (off_t)inode->i_size)
	{
		// Do nothing
		return 0;
	}

	vsfs_blk_t num_blks_old = inode->i_blocks;
	vsfs_blk_t num_blks_new = div_round_up(size, VSFS_BLOCK_SIZE);

	vsfs_blk_t blk_end_pos_old = (inode->i_size - 1) % VSFS_BLOCK_SIZE;
	vsfs_blk_t blk_end_pos_new = (size - 1) % VSFS_BLOCK_SIZE;

	// check if the new size is smaller than the old size
	if (size < (off_t)inode->i_size)
	{
		// shrink the file
		for (vsfs_blk_t i = num_blks_new; i < num_blks_old; i++)
		{
			if (i < VSFS_NUM_DIRECT)
			{
				// shrink Direct data blocks
				vsfs_blk_t *block = (vsfs_blk_t *)(fs->image + inode->i_direct[i] * VSFS_BLOCK_SIZE);

				// shrink the other blocks
				memset(block, 0, VSFS_BLOCK_SIZE);
				bitmap_free(fs->dbmap, nblks, inode->i_direct[i]);
				fs->sb->sb_free_blocks++;
				inode->i_direct[i] = VSFS_BLK_UNASSIGNED;
			}
			else
			{
				// shrink indirect data blocks
				vsfs_blk_t *indirect_data = (vsfs_blk_t *)(fs->image + inode->i_indirect * VSFS_BLOCK_SIZE);
				vsfs_blk_t *block = (vsfs_blk_t *)(fs->image + indirect_data[i - VSFS_NUM_DIRECT] * VSFS_BLOCK_SIZE);
				if (i == num_blks_new - 1)
				{
					// shrink the last block to the last byte of the new size
					memset(block + blk_end_pos_new + 1, 0, VSFS_BLOCK_SIZE - blk_end_pos_new - 1);
				}
				else
				{
					// shrink the other blocks
					memset(block, 0, VSFS_BLOCK_SIZE);
					bitmap_free(fs->dbmap, nblks, indirect_data[i - VSFS_NUM_DIRECT]);
					fs->sb->sb_free_blocks++;
					indirect_data[i - VSFS_NUM_DIRECT] = VSFS_BLK_UNASSIGNED;
				}
			}
		}
		if ((num_blks_old > VSFS_NUM_DIRECT) && (num_blks_new <= VSFS_NUM_DIRECT))
		{
			// shrink the indirect block, since the the new file size would only use direct blocks
			vsfs_blk_t *indirect_data = (vsfs_blk_t *)(fs->image + inode->i_indirect * VSFS_BLOCK_SIZE);
			memset(indirect_data, 0, VSFS_BLOCK_SIZE);
			bitmap_free(fs->dbmap, nblks, inode->i_indirect);
			fs->sb->sb_free_blocks++;
			inode->i_indirect = VSFS_BLK_UNASSIGNED;
		}
	}
	else
	{
		// extend the file
		// check if there is enough free blocks to extend the file
		bool not_enough_free_blocks = (num_blks_new - num_blks_old) > fs->sb->sb_free_blocks;
		bool not_enough_free_blocks_indirect = (num_blks_new > VSFS_NUM_DIRECT) && (num_blks_old <= VSFS_NUM_DIRECT) && ((num_blks_new - num_blks_old + 1) > fs->sb->sb_free_blocks);
		if (not_enough_free_blocks || not_enough_free_blocks_indirect)
		{
			// not enough free blocks
			return -ENOSPC;
		}

		for (vsfs_blk_t i = 0; i < nblks; i++)
		{
			if (!bitmap_isset(fs->dbmap, nblks, i))
			{
				memset(fs->image + i * VSFS_BLOCK_SIZE, 0, VSFS_BLOCK_SIZE);
			}
		}

		// check if we need to allocate a new indirect block
		if ((num_blks_new > VSFS_NUM_DIRECT) && (num_blks_old <= VSFS_NUM_DIRECT))
		{
			// allocate a new indirect block
			vsfs_blk_t indirect_blk;
			assert(bitmap_alloc(fs->dbmap, nblks, &indirect_blk) == 0);
			inode->i_indirect = indirect_blk;
			fs->sb->sb_free_blocks--;
			bitmap_set(fs->dbmap, nblks, indirect_blk, true);
		}
		// extend the file
		for (vsfs_blk_t i = num_blks_old; i < num_blks_new; i++)
		{
			if (i < VSFS_NUM_DIRECT)
			{
				// extend Direct data blocks
				vsfs_blk_t direct_blk_index;
				assert(bitmap_alloc(fs->dbmap, nblks, &direct_blk_index) == 0);
				inode->i_direct[i] = direct_blk_index;
				fs->sb->sb_free_blocks--;
				bitmap_set(fs->dbmap, nblks, direct_blk_index, true);
				memset(fs->image + direct_blk_index * VSFS_BLOCK_SIZE, 0, VSFS_BLOCK_SIZE);
			}
			else
			{
				// extend indirect data blocks
				vsfs_blk_t *indirect_data = (vsfs_blk_t *)(fs->image + inode->i_indirect * VSFS_BLOCK_SIZE);
				if (i == num_blks_old - 1)
				{
					// extend the last block starting last byte of the old size
					vsfs_blk_t *block = (vsfs_blk_t *)(fs->image + indirect_data[i - VSFS_NUM_DIRECT] * VSFS_BLOCK_SIZE);
					memset(block + blk_end_pos_old + 1, 0, VSFS_BLOCK_SIZE - blk_end_pos_old - 1);
				}
				else
				{
					// i >= num_blks_old, extend the other blocks
					vsfs_blk_t indirect_blk_index;
					assert(bitmap_alloc(fs->dbmap, nblks, &indirect_blk_index) == 0);
					indirect_data[i - VSFS_NUM_DIRECT] = indirect_blk_index;
					fs->sb->sb_free_blocks--;
					bitmap_set(fs->dbmap, nblks, indirect_blk_index, true);
					memset(fs->image + indirect_blk_index * VSFS_BLOCK_SIZE, 0, VSFS_BLOCK_SIZE);
				}
			}
		}
	}

	// update the file size, # blocks, and mtime
	inode->i_size = size;
	inode->i_blocks = num_blks_new;
	assert(clock_gettime(CLOCK_REALTIME, &inode->i_mtime) == 0);

	return 0;
}

/**
 * Read data from a file.
 *
 * Implements the pread() system call. Must return exactly the number of bytes
 * requested except on EOF (end of file). Reads from file ranges that have not
 * been written to must return ranges filled with zeros. You can assume that the
 * byte range from offset to offset + size is contained within a single block.
 *
 * Assumptions (already verified by FUSE using getattr() calls):
 *   "path" exists and is a file.
 *
 * Errors: none
 *
 * @param path    path to the file to read from.
 * @param buf     pointer to the buffer that receives the data.
 * @param size    buffer size (number of bytes requested).
 * @param offset  offset from the beginning of the file to read from.
 * @param fi      unused.
 * @return        number of bytes read on success; 0 if offset is beyond EOF;
 *                -errno on error.
 */
static int vsfs_read(const char *path, char *buf, size_t size, off_t offset,
					 struct fuse_file_info *fi)
{
	(void)fi; // unused
	fs_ctx *fs = get_fs();

	// TODO: read data from the file at given offset into the buffer
	vsfs_ino_t ino;
	int ret = path_lookup(path, &ino, NULL);
	assert(ret == 0);
	vsfs_inode *inode = get_inode(ino);
	assert(ino != VSFS_ROOT_INO);
	// check if the offset is beyond EOF - read nothing
	if (offset >= (off_t)inode->i_size)
	{
		return 0;
	}

	// calculate which block to read from and which byte to read from
	vsfs_blk_t blk_index = offset / VSFS_BLOCK_SIZE;
	vsfs_blk_t blk_offset = offset % VSFS_BLOCK_SIZE;

	// check if bytes read is less than the size of the buffer, if so, read until EOF
	size_t bytes_to_read = size;
	if ((blk_index == inode->i_blocks - 1) && (size + blk_offset > VSFS_BLOCK_SIZE))
	{
		bytes_to_read = VSFS_BLOCK_SIZE - blk_offset;
	}

	// find the actual block to read from
	vsfs_blk_t actual_blk_number;
	if (blk_index < VSFS_NUM_DIRECT)
	{
		actual_blk_number = inode->i_direct[blk_index];
	}
	else
	{
		vsfs_blk_t *indirect_data = (vsfs_blk_t *)(fs->image + inode->i_indirect * VSFS_BLOCK_SIZE);
		actual_blk_number = indirect_data[blk_index - VSFS_NUM_DIRECT];
	}

	// read from the actual block + bytes to read
	memcpy(buf, fs->image + actual_blk_number * VSFS_BLOCK_SIZE + blk_offset, bytes_to_read);
	return bytes_to_read;
}

/**
 * Write data to a file.
 *
 * Implements the pwrite() system call. Must return exactly the number of bytes
 * requested except on error. If the offset is beyond EOF (end of file), the
 * file must be extended. If the write creates a "hole" of uninitialized data,
 * the new uninitialized range must filled with zeros. You can assume that the
 * byte range from offset to offset + size is contained within a single block.
 *
 * Assumptions (already verified by FUSE using getattr() calls):
 *   "path" exists and is a file.
 *
 * Errors:
 *   ENOMEM  not enough memory (e.g. a malloc() call failed).
 *   ENOSPC  not enough free space in the file system.
 *   EFBIG   write would exceed the maximum file size
 *
 * @param path    path to the file to write to.
 * @param buf     pointer to the buffer containing the data.
 * @param size    buffer size (number of bytes requested).
 * @param offset  offset from the beginning of the file to write to.
 * @param fi      unused.
 * @return        number of bytes written on success; -errno on error.
 */
static int vsfs_write(const char *path, const char *buf, size_t size,
					  off_t offset, struct fuse_file_info *fi)
{
	(void)fi; // unused
	fs_ctx *fs = get_fs();

	// TODO: write data from the buffer into the file at given offset, possibly
	//  "zeroing out" the uninitialized range
	vsfs_ino_t ino;
	int ret = path_lookup(path, &ino, NULL);
	assert(ret == 0);
	vsfs_inode *inode = get_inode(ino);
	assert(ino != VSFS_ROOT_INO);

	// Use truncate() to extend the file if the offset + size is beyond EOF
	if (offset + size > inode->i_size)
	{
		int ret = vsfs_truncate(path, offset + size);
		if (ret < 0)
		{
			// No free space
			return ret;
		}
	}
	// calculate which block to write to and which byte to write to
	vsfs_blk_t blk_index = offset / VSFS_BLOCK_SIZE;
	vsfs_blk_t blk_offset = offset % VSFS_BLOCK_SIZE;

	// find the actual block to write to
	vsfs_blk_t actual_blk_number;
	if (blk_index < VSFS_NUM_DIRECT)
	{
		actual_blk_number = inode->i_direct[blk_index];
	}
	else
	{
		vsfs_blk_t *indirect_data = (vsfs_blk_t *)(fs->image + inode->i_indirect * VSFS_BLOCK_SIZE);
		actual_blk_number = indirect_data[blk_index - VSFS_NUM_DIRECT];
	}
	// write to the actual block + bytes to write
	memcpy(fs->image + actual_blk_number * VSFS_BLOCK_SIZE + blk_offset, buf, size);
	// update mtime
	assert(clock_gettime(CLOCK_REALTIME, &inode->i_mtime) == 0);

	return size;
}

static struct fuse_operations vsfs_ops = {
	.destroy = vsfs_destroy,
	.statfs = vsfs_statfs,
	.getattr = vsfs_getattr,
	.readdir = vsfs_readdir,
	.create = vsfs_create,
	.unlink = vsfs_unlink,
	.utimens = vsfs_utimens,
	.truncate = vsfs_truncate,
	.read = vsfs_read,
	.write = vsfs_write,
};

int main(int argc, char *argv[])
{
	vsfs_opts opts = {0}; // defaults are all 0
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	if (!vsfs_opt_parse(&args, &opts))
		return 1;

	fs_ctx fs = {0};
	if (!vsfs_init(&fs, &opts))
	{
		fprintf(stderr, "Failed to mount the file system\n");
		return 1;
	}

	return fuse_main(args.argc, args.argv, &vsfs_ops, &fs);
}
