{application, dynomite,
  [{description, "Dynomite Storage Node"},
   {mod, {dynomite_app, {3,
      [
        {fs_storage, "/Users/cliff/data/storage_test", fsstore}
      ]}
    }}
  ]}.