package skbn

import (
	"context"
	"fmt"
	"log"
	"math"
	"path/filepath"

	"github.com/Cognologix/skbn/pkg/utils"
)

// Delete files from path
func Delete(path string, parallel int) error {
	prefix, path := utils.SplitInTwo(path, "://")

	err := TestImplementationsExistForDelete(prefix)
	if err != nil {
		return err
	}
	client, err := GetClient(prefix, path)
	if err != nil {
		return err
	}
	relativePaths, err := GetListOfFiles(client, prefix, path)
	if err != nil {
		return err
	}

	var absolutePaths []string
	for _, relativePath := range relativePaths {
		absolutePaths = append(absolutePaths, filepath.Join(path, relativePath))
	}

	err = PerformDelete(client, prefix, absolutePaths, parallel)
	if err != nil {
		return err
	}

	return nil
}

// TestImplementationsExistForDelete checks that implementations exist for the desired action
func TestImplementationsExistForDelete(prefix string) error {
	switch prefix {
	case "k8s":
	//case "s3":
	//case "abs":
	//case "gcs":
	default:
		return fmt.Errorf(prefix + " not implemented")
	}

	return nil
}

// GetClient gets the client for cloud service provider
func GetClient(prefix, path string) (interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, _, err := initClient(ctx, nil, prefix, path, "")
	if err != nil {
		return nil, err
	}

	return client, nil
}

// PerformDelete performs the actual delete action
func PerformDelete(client interface{}, prefix string, paths []string, parallel int) error {
	// Execute in parallel
	totalFiles := len(paths)
	if parallel == 0 {
		parallel = totalFiles
	}
	bwgSize := int(math.Min(float64(parallel), float64(totalFiles))) // Very stingy :)
	bwg := utils.NewBoundedWaitGroup(bwgSize)
	errc := make(chan error, 1)
	currentLine := 0
	for _, path := range paths {

		if len(errc) != 0 {
			break
		}

		bwg.Add(1)
		currentLine++

		totalDigits := utils.CountDigits(totalFiles)
		currentLinePadded := utils.LeftPad2Len(currentLine, 0, totalDigits)

		go func(client interface{}, prefix, path, currentLinePadded string, totalFiles int) {
			defer bwg.Done()

			if len(errc) != 0 {
				return
			}

			log.Printf("[%s/%d] delete: %s://%s", currentLinePadded, totalFiles, prefix, path)

			err := DeleteFile(client, prefix, path)
			if err != nil {
				log.Println(err, fmt.Sprintf(" file: %s", path))
				errc <- err
			}
		}(client, prefix, path, currentLinePadded, totalFiles)
	}
	bwg.Wait()
	if len(errc) != 0 {
		// This is not exactly the correct behavior
		// There may be more than 1 error in the channel
		// But first let's make it work
		err := <-errc
		close(errc)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteFile deletes a single file from path
func DeleteFile(client interface{}, prefix, path string) error {
	/*ctx, cancel := context.WithCancel(context.Background())
	defer cancel()*/

	switch prefix {
	case "k8s":
		err := DeleteFromK8s(client, path)
		if err != nil {
			return err
		}
	/* For now only k8s and S3 are supported
	case "s3":
		err := DownloadFromS3(srcClient, srcPath, writer)
		if err != nil {
			return err
		}
	case "abs":
		err := DownloadFromAbs(ctx, srcClient, srcPath, writer)
		if err != nil {
			return err
		}
	case "gcs":
		err := DownloadFromGcs(ctx, srcClient, srcPath, writer)
		if err != nil {
			return err
		}*/
	default:
		return fmt.Errorf(prefix + " not implemented")
	}

	return nil
}
