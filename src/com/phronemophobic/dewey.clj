(ns com.phronemophobic.dewey
  (:require [clojure.set :as set]
            [com.phronemophobic.dewey.util
             :refer [copy read-edn with-auth ->edn]
             :as util]
            [clj-http.client :as http]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import java.time.Instant
           java.time.Duration
           java.time.format.DateTimeFormatter
           java.io.PushbackReader
           (java.util.regex Pattern)))


(def api-base-url "https://api.github.com")

(def search-repos-url (str api-base-url "/search/repositories"))

(def base-request
  {:url          search-repos-url
   :method       :get
   :as           :json
   :query-params {:per_page 100
                  :sort     "updated"
                  :order    "asc"}})

(defn release-dir [release-id]
  (let [dir (io/file "releases" release-id)]
    (.mkdirs dir)
    dir))

(defn search-repos-request [query]
  (assoc-in base-request [:query-params :q] query))

(defn list-releases []
  (let [list-release-req
        {:url          (str api-base-url
                            "/repos/phronmophobic/dewey/releases")
         :method       :get
         :content-type :json
         :as           :json}]
    (http/request (with-auth list-release-req))))

(defn publish-release [release-id]
  (let [publish-release-req
        {:url          (str api-base-url
                            "/repos/phronmophobic/dewey/releases/"
                            release-id)
         :method       :patch
         :content-type :json
         :form-params  {"draft" false}
         :as           :json}]
    (http/request (with-auth publish-release-req))))

(defn make-github-release [release-id sha files]
  (let [make-tag-req
        {:url          (str api-base-url
                            "/repos/phronmophobic/dewey/git/tags")
         :method       :post
         :content-type :json
         :form-params
         {"tag"     release-id
          "message" "Release",
          "object"  sha
          "type"    "commit",}
         :as           :json}
        make-tag-response (http/request (with-auth make-tag-req))
        ;; _ (clojure.pprint/pprint make-tag-response)

        make-release-req
        {:url          (str api-base-url
                            "/repos/phronmophobic/dewey/releases")
         :method       :post
         :content-type :json
         :form-params  {"tag_name"         release-id
                        "target_commitish" "main"
                        "name"             release-id
                        "draft"            true
                        "prerelease"       false}
         :as           :json}
        make-release-response (http/request (with-auth make-release-req))
        github-release-id (-> make-release-response
                              :body
                              :id)]
    ;; (clojure.pprint/pprint make-release-response)
    (assert github-release-id)
    (doseq [file files]
      (let [upload-req
            {:url          (str "https://uploads.github.com/repos/phronmophobic/dewey/releases/" github-release-id "/assets")
             :headers      {"Content-Type" "application/octet-stream"}
             :method       :post
             :query-params {:name (.getName file)}
             :body         file
             :as           :json}]
        (prn "uploading" (.getName file))
        (http/request (with-auth upload-req))))

    (publish-release github-release-id)))

(defn rate-limit-sleep! [response]
  (when response
    (let [headers (:headers response)
          rate-limit-remaining (Long/parseLong (get headers "X-RateLimit-Remaining"))
          rate-limit-reset (-> headers
                               (get "X-RateLimit-Reset")
                               (Long/parseLong)
                               (Instant/ofEpochSecond))]
      (println "limit remaining " rate-limit-remaining)
      (when (<= rate-limit-remaining 0)
        (let [duration (Duration/between (Instant/now)
                                         rate-limit-reset)
              duration-ms (+ 1000 (.toMillis duration))]
          (when (pos? duration-ms)
            (prn "sleeping " duration-ms)
            (Thread/sleep duration-ms)))))))

(defn with-retries [step]
  (fn [m]
    (let [result
          (try+
            (step m)
            (catch [:status 500] e
              ::error)
            (catch [:status 502] e
              ::error))]
      (if (= result ::error)
        (let [error-count (get m ::error-count 0)]
          (if (< error-count 3)
            (do
              ;; sleep for a second and then retry
              (prn "received 500 error. retrying...")
              (Thread/sleep 1000)
              (recur (assoc m ::error-count (inc error-count))))
            (throw (ex-info "Failed after retries"
                            {:k m}))))
        result))))

(defmacro with-timing [name exp]
  `(do
     (print "Time for" ~name "... ")
     (flush)
     (let [start# (System/currentTimeMillis)
           res# ~exp]
       (println (- (System/currentTimeMillis) start#) "ms")
       res#)))

(defn slim-item [item]
  (select-keys item [:full_name :pushed_at]))

(defn save-to-disk! [items {:keys [session-index] :as cursor}]
  (doseq [{:keys [full_name pushed_at] :as item} items]
    (spit "all-repos.edn"
          (str (pr-str (array-map :session-index session-index
                                  :full_name full_name
                                  :pushed_at pushed_at
                                  :cursor (dissoc cursor :session-index)
                                  :item item)) "\n")
          :append true)))

(defn all-repos []
  (if-not (.exists (io/file "all-repos.edn"))
    []
    (with-open [rdr (io/reader "all-repos.edn")]
      (mapv edn/read-string (line-seq rdr)))))

(defn last-item []
  (last (all-repos)))

(defn fetch-one [{:keys [url pushed_at request-count last-response]
                  :or {request-count 0}
                  :as k}]
  (prn request-count (select-keys k [:url :pushed_at]))
  (let [req (cond
              ;; received next-url
              url (assoc base-request :url url)
              ;; received pushed_at
              pushed_at (search-repos-request (str "language:clojure pushed:=" pushed_at))
              ;; initial request
              (= request-count 0) (search-repos-request "language:clojure")

              :else (throw (Exception. (str "Unexpected key type: " (pr-str k)))))]
       (rate-limit-sleep! last-response)
       (http/request (with-auth req))))

(defn next-k [{:keys [request-count page-count max-requests new-items] :as response}]
  (def r response)
  (let [next-url (-> response :links :next :href)]
    (if (= request-count max-requests)
      (do
        (println "max requests reached, stopping")
        nil)
      (when-let [m (if next-url
                     {:url next-url}
                     (when-let [pushed_at (some-> new-items last :pushed_at)]
                       {:pushed_at pushed_at}))]
        (merge m
               (select-keys response [:session-index :all-items :max-requests :request-count])
               {:last-response response})))))

(defn find-clojure-repos []
  (iteration
    (with-retries
      (fn [{:keys [all-items request-count last-response save-to-disk?] :as k}]
        (let [start-time (System/currentTimeMillis)]
          (rate-limit-sleep! last-response)
          (let [response (fetch-one k)
                new-items (->> (get-in response [:body :items] [])
                               (remove #(contains? all-items (slim-item %)))
                               (vec))
                new-all-items (set/union all-items (->> new-items
                                                        (mapv slim-item)
                                                        (into #{})))]
            (when save-to-disk?
              (save-to-disk! new-items (select-keys k [:session-index :url :pushed_at])))
            (println "new items:" (count new-items) ", total items:" (count new-all-items)
                     ", spent" (- (System/currentTimeMillis)
                                  start-time)
                     "ms")
            (-> response
                (assoc :request-count (inc request-count))
                (assoc :all-items new-all-items)
                (assoc :new-items new-items)
                (assoc :page-count (count (get-in response [:body :items] [])))
                (assoc :max-requests (get k :max-requests))
                (assoc :session-index (get k :session-index)))))))
    :kf (fn [resp] (next-k resp))
    :initk (merge {:request-count 0
                   :max-requests  1
                   :session-index ((fnil inc 0) (:session-index (last-item)))
                   :all-items     (->> (all-repos)
                                       (mapv slim-item)
                                       (into #{}))}
                  (:cursor (last-item)))))

(comment
  (def all-repos-vec (vec (find-clojure-repos))))

(comment
  (def last-page (last (find-clojure-repos))))

(comment
  (def repo (->> (find-clojure-repos)
                 (first)
                 :body
                 :items
                 (first)
                 (into (sorted-map)))))

(defn load-all-repos [release-id]
  (read-edn (io/file (release-dir release-id) "all-repos.edn")))

(defn fname-url [repo fname]
  (let [full-name (:full_name repo)
        ref (:git/sha repo)]
    (str "https://raw.githubusercontent.com/" full-name "/" ref "/" fname)))

(defn sanitize [s]
  (str/replace s #"[^a-zA-Z0-9_.-]" "_"))

(defn repo->file [repo dir fname]
  (io/file dir
           (sanitize (-> repo :owner :login))
           (sanitize (:name repo))
           fname))

(defn download-file
  ([{:keys [repos
            fname
            release-id
            dirname]}]
   (assert (and repos
                fname
                dirname))
   (let [repo-count (count repos)
         ;; default rate limit is 5k/hour
         ;; aiming for 4.5k/hour since there's no good feedback mechanism
         chunks (partition-all 4500
                               (map-indexed vector repos))
         fname-dir (io/file (release-dir release-id) dirname)]
     (.mkdirs fname-dir)
     (doseq [[chunk sleep?] (map vector
                                 chunks
                                 (concat (map (constantly true) (butlast chunks))
                                         [false]))]
       (doseq [[i repo] chunk]
         (try+
           (let [name (:name repo)
                 owner (-> repo :owner :login)
                 _ (print i "/" repo-count " checking " name owner "...")
                 result (http/request (with-auth
                                        {:url    (fname-url repo fname)
                                         :method :get
                                         :as     :stream}))
                 output-file (repo->file repo fname-dir fname)]
             (.mkdirs (.getParentFile output-file))
             (println "found.")
             (copy (:body result)
                   output-file
                   ;; limit file sizes to 50kb

                   (* 50 1024))
             (.close (:body result)))
           (catch [:status 404] {:keys [body]}
             (println "not found"))
           (catch [:type :max-bytes-limit-exceeded] _
             (println "file too big! skipping..."))))

       (when sleep?
         (println "sleeping for an hour")
         ;; sleep an hour
         (dotimes [i 60]
           (println (- 60 i) " minutes until next chunk.")
           (Thread/sleep (* 1000 60))))))))

(defn download-deps
  ([opts]
   (let [release-id (:release-id opts)
         repos (->> (util/read-edn (io/file (release-dir release-id)
                                            "default-branches.edn"))
                    (into
                      []
                      (comp (map (fn [[repo branch-info]]
                                   (assoc repo
                                     :git/sha (-> branch-info
                                                  :commit
                                                  :sha))))
                            (filter :git/sha))))]
     (assert release-id)
     (download-file {:fname      "deps.edn"
                     :dirname    "deps"
                     :release-id release-id
                     :repos      repos}))))



;; (repos/repos {:auth auth :per-page 1})




(defn parse-edn [f]
  (try
    (with-open [rdr (io/reader f)
                rdr (PushbackReader. rdr)]
      (edn/read rdr))
    (catch Exception e
      (prn f e)
      nil)))

(defn update-clojure-repo-index [{:keys [release-id]}]
  (assert release-id)
  (let [all-responses (vec
                        (find-clojure-repos))
        all-repos (->> all-responses
                       (map :body)
                       (mapcat :items))]
    (spit (io/file (release-dir release-id) "all-repos.edn")
          (->edn all-repos))))


(defn archive-zip-url
  ([owner repo]
   (archive-zip-url owner repo nil))
  ([owner repo ref]
   (str api-base-url "/repos/" owner "/" repo "/zipball/" ref)))


(comment
  (def tags
    (http/request (with-auth
                    {:url    tag-url
                     :as     :json
                     :method :get}))),)

(defn find-tags [repos]
  (iteration
    (with-retries
      (fn [{:keys [repos last-response] :as k}]
        (let [repo (first repos)
              req {:url    (:tags_url repo)
                   :as     :json
                   :method :get}]
          (prn req)
          (rate-limit-sleep! last-response)
          (let [response (try+
                           (http/request (with-auth req))
                           (catch [:status 404] e
                             {:body []}))]
            (assoc response
              ::repo repo
              ::key k
              ::request req)))))
    :vf (juxt ::repo :body)
    :kf
    (fn [response]
      (when-let [next-repos (-> response
                                ::key
                                :repos
                                next)]
        {:last-response response
         :repos         next-repos}))
    :initk {:repos (seq repos)}))



(defn update-tag-index [{:keys [release-id]}]
  (assert release-id)
  (let [all-repos (load-all-repos release-id)
        deps-repos (->> all-repos
                        (filter (fn [repo]
                                  (.exists (repo->file repo
                                                       (io/file (release-dir release-id) "deps")
                                                       "deps.edn")))))
        all-tags (vec (find-tags deps-repos))]
    (spit (io/file (release-dir release-id) "deps-tags.edn")
          (->edn all-tags))))

(defn load-deps-tags [release-id]
  (read-edn (io/file (release-dir release-id) "deps-tags.edn")))

(defn branch-test [repo]
  (str/replace (:branches_url repo)
               #"\{/branch}"
               (str "/" (:default_branch repo))))

(defn find-default-branches [repos]
  (iteration
    (with-retries
      (fn [{:keys [repos last-response] :as k}]
        (let [repo (first repos)
              req {:url                  (str/replace (:branches_url repo)
                                                      #"\{/branch}"
                                                      (str "/" (:default_branch repo)))
                   :unexceptional-status #(or (http/unexceptional-status? %)
                                              (= % 404))
                   :as                   :json
                   :method               :get}]
          (rate-limit-sleep! last-response)
          (let [response (http/request (with-auth req))]
            (assoc response
              ::repo repo
              ::key k
              ::request req)))))
    :vf (juxt ::repo :body)
    :kf
    (fn [response]
      (when-let [next-repos (-> response
                                ::key
                                :repos
                                next)]
        {:last-response response
         :repos         next-repos}))
    :initk {:repos (seq repos)}))

(defn update-default-branches [{:keys [release-id]}]
  (assert release-id)
  (let [all-repos (load-all-repos release-id)
        all-default-branches (vec (find-default-branches all-repos))]
    (util/save-obj-edn (io/file (release-dir release-id) "default-branches.edn")
                       all-default-branches)))

(defn load-available-git-libs [release-id]
  (read-edn (io/file (release-dir release-id)) "deps-libs.edn"))

(defn update-available-git-libs-index [{:keys [release-id]}]
  (assert release-id)
  (let [deps-tags (load-deps-tags release-id)
        deps-libs
        (into {}
              (map (fn [[repo tags]]
                     ;;io.github.yourname/time-lib {:git/tag "v0.0.1" :git/sha "4c4a34d"}
                     (let [login (-> repo :owner :login)
                           repo-name (:name repo)
                           repo-name (if (not (re-matches #"^[a-zA-Z].*" repo-name))
                                       (str "X-" repo-name)
                                       repo-name)
                           lib (symbol (str "io.github." login) repo-name)
                           versions
                           (vec
                             (for [tag tags]
                               {:git/tag (:name tag)
                                :git/sha (-> tag :commit :sha)}))]
                       [lib
                        {:description (:description repo)
                         :lib         lib
                         :topics      (:topics repo)
                         :stars       (:stargazers_count repo)
                         :url         (:html_url repo)
                         :versions    versions}])))
              deps-tags)]
    (spit (io/file (release-dir release-id) "deps-libs.edn")
          (->edn deps-libs))))

(defn make-release [{:keys [release-id]
                     :as   opts}]
  (assert release-id)
  (update-clojure-repo-index opts)
  (download-deps opts)
  (update-tag-index opts)
  (update-available-git-libs-index opts))
