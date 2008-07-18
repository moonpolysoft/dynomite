{application, dynomite,
  [{description, "Dynomite Storage Node"},
   {mod, {dynomite_app, {{1,1,2},
      [
        {fs_storage, "/Users/cliff/data/storage_test", fsstore}
      ]}
    }}
  ]}.