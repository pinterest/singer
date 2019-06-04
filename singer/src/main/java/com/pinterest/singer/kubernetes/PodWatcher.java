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
package com.pinterest.singer.kubernetes;

/**
 * Pod Watcher is an interface that if implemented and registered with the
 * KubeService will be notified of Kubernetes POD events.
 * 
 * Note: these events are triggered when a delta is detected during the periodic
 * kubernetes service poll
 * 
 * Following a listener design, currently we only have 1 listener but in future
 * if we want to do something else as well when these events happen then this
 * might come in handy.
 */
public interface PodWatcher {

    /**
     * Called if a new Pod is added to the host
     * 
     * @param podUid
     */
    public void podCreated(String podUid);

    /**
     * Called if a Pod is deleted / terminated
     * 
     * @param podUid
     */
    public void podDeleted(String podUid);

}
