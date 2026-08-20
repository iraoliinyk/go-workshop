package batch

// BatchSlice divides a slice into batches of at most batchSize.
func BatchSlice[T any](slice []T, batchSize int) [][]T {
	if batchSize <= 0 {
		return nil
	}

	var batches [][]T
	for batchSize < len(slice) {
		slice, batches = slice[batchSize:], append(batches, slice[0:batchSize:batchSize])
	}
	batches = append(batches, slice)

	return batches
}
