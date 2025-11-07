// Copyright (C) 2025 GerritForge, Inc.
//
// Licensed under the BSL 1.1 (the "License");
// you may not use this file except in compliance with the License.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.replication.pull;

import com.google.gson.TypeAdapterFactory;
import com.ryanharter.auto.value.gson.GsonTypeAdapterFactory;

@GsonTypeAdapterFactory
public abstract class AutoValueTypeAdapterFactory implements TypeAdapterFactory {

  public static TypeAdapterFactory create() {
    return new AutoValueGson_AutoValueTypeAdapterFactory();
  }
}
