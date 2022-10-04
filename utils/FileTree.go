package utils

import (
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage_base"
	"strings"
)

type Dir struct {
	name  string // empty for the root dir
	files map[string]File
	dirs  map[string]*Dir
}

type File struct {
	path         string
	modifiedTime uint64
	flags        int32
	size         uint64

	blob_path   string
	blob_offset int64
	compAlgo    string
	final_size  int64
	storageID   *[]byte
	key         *[]byte
}

const (
	QUERY = `
		SELECT
       		files.path,
       		files.fs_modified,
       		files.permissions,
       		sizes.size,
       		
       		blob_storage.path,
       		blob_entries.offset,
       		blob_entries.compression_alg,
			blob_entries.final_size,
			storage.storage_id,
			blobs.encryption_key
		FROM files
			INNER JOIN sizes ON sizes.hash = files.hash
			INNER JOIN blob_entries ON blob_entries.hash = files.hash
			INNER JOIN blobs ON blobs.blob_id = blob_entries.blob_id
			INNER JOIN blob_storage ON blob_storage.blob_id = blobs.blob_id
			INNER JOIN storage ON storage.storage_id = blob_storage.storage_id
		WHERE (? >= files.start AND (files.end > ? OR files.end IS NULL)) AND files.path GLOB ? AND blob_storage.storage_id = ?`
)

func queryAllFiles(path string, timestamp int64, storage storage_base.Storage) []File {
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
	}()

	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	rows, err := tx.Query(QUERY, timestamp, timestamp, path+"*", storage.GetID())
	if err != nil {
		panic(err)
	}
	var files []File
	for rows.Next() {
		var file File
		err = rows.Scan(&file.path, &file.modifiedTime, &file.flags, &file.size, &file.blob_path, &file.blob_offset, &file.compAlgo, &file.final_size, &file.storageID, &file.key)
		if err != nil {
			panic(err)
		}
		files = append(files, file)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return files
}
