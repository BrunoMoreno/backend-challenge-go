package postgres

import (
	"context"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/storage"
)

// Database adapta o UnitOfWorkFactory à porta storage.Database usada pelos
// casos de uso. O UnitOfWork devolvido expõe repositórios com as interfaces da
// aplicação; o campo concreto continua disponível para uso direto.
type Database struct {
	Factory *UnitOfWorkFactory
}

// NewDatabase cria o adaptador de banco para os casos de uso.
func NewDatabase(f *UnitOfWorkFactory) *Database {
	return &Database{Factory: f}
}

// Begin abre uma transação no formato dos casos de uso.
func (d *Database) Begin(ctx context.Context) (storage.UnitOfWork, error) {
	return d.Factory.Begin(ctx)
}

// BeginReadOnly abre uma transação de leitura consistente (REPEATABLE READ)
// para a reconciliação.
func (d *Database) BeginReadOnly(ctx context.Context) (storage.UnitOfWork, error) {
	return d.Factory.BeginReadOnly(ctx)
}

// Compile-time: o adaptador implementa a porta do domínio de aplicação.
var _ storage.Database = (*Database)(nil)
var _ storage.UnitOfWork = (*UnitOfWork)(nil)
