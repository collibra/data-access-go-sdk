package utils

func CompareStringPointers(p1, p2 *string) bool {
	if p1 == nil && p2 == nil {
		return true
	}

	if p1 == nil || p2 == nil {
		return false
	}

	return *p1 == *p2
}
