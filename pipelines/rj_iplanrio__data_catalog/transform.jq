group_by(.projeto, .dataset, .tabela) |
map({
  projeto: .[0].projeto,
  dataset: .[0].dataset,
  tabela: .[0].tabela,
  table_type: .[0].table_type,
  created_at: .[0].criado_em,
  description: (.[0].descricao_tabela // "" | gsub("^\\\"|\\\"$"; "")),
  schema: (map({
    (.coluna): {
      description: (.descricao_coluna // "" | gsub("^\\\"|\\\"$"; "")),
      type: .tipo_dado
    }
  }) | add)
}) |
group_by(.projeto) |
map({
  (.[0].projeto): (
    group_by(.dataset) |
    map({
      (.[0].dataset): {
        description: (.[0].description // "" | gsub("^\\\"|\\\"$"; "")),
        tables: map({
          name: .tabela,
          created_at: .created_at,
          schema: .schema
        })
      }
    }) | add
  )
}) | add