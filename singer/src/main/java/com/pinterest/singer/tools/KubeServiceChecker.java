/**
 * Copyright 2019 Pinterest, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.singer.tools;

import java.util.Set;

import com.pinterest.singer.common.SingerSettings;
import com.pinterest.singer.kubernetes.KubeService;
import com.pinterest.singer.thrift.configuration.KubeConfig;
import com.pinterest.singer.thrift.configuration.SingerConfig;

/**
 * Simple command line tool for checking Pod IDs from kubernetes
 */
public class KubeServiceChecker {

	public static void main(String[] args) {
		SingerConfig singerConfig = new SingerConfig();
		singerConfig.setKubernetesEnabled(true);
		singerConfig.setKubeConfig(new KubeConfig());
        SingerSettings.setSingerConfig(singerConfig);
		
		KubeService service = KubeService.getInstance();
		try {
			Set<String> podUids = service.fetchPodNamesFromMetadata();
			for (String puid : podUids) {
				System.out.println(puid);
			}
		} catch (Exception e) {
			System.err.println("Failed to fetch Pod IDs. Reason:"+e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
