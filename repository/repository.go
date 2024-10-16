package repository

import (
	"github.com/PlakarLabs/plakar/storage"
)

type Repository struct {
	store *storage.Store
}

func New(store *storage.Store) *Repository {
	return &Repository{store: store}
}

func (r *Repository) Store() *storage.Store {
	return r.store
}

func (r *Repository) Configuration() storage.Configuration {
	return r.store.Configuration()
}
