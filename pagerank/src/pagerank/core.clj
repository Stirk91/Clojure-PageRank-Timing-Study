(ns pagerank.core
  (:gen-class) 
  (:require [clojure.pprint :as pp])
  (:import (java.util.concurrent Executors)))

(use 'clojure.java.io)
(use '[clojure.string :as str :only [split]])
(require '[clojure.edn :as edn])



(defn printme [input]
  (pp/pprint input))


(defn combine-maps [mapSeq]
  (into (sorted-map) mapSeq)) ;; converts lazy seq of maps to one map


(defn save-results [map n save]
  (if (true? save)
    (spit "resources/output.txt" (str "PageRank results after " n " iterations (PageID PageRank): \n\n" map))))


(defn save-times [t s time]
  (if (< t 10)
    (spit "resources/output_times.txt" (str "Threads:  " t " | Map Partitions: " s " | " time) :append true)
    (spit "resources/output_times.txt" (str "Threads: " t " | Map Partitions: " s " | " time) :append true)))    


(defn start-rank [map n]
  (combine-maps (for [[pageID pagerank] map]
                  (into {} {pageID n}))))

;; searches entire hash map for each page ID
(defn search-map [targetID map]
  (for [[pageIndex outpages] map] ;; for every entry in the map
    (if (some #(= targetID %) (get map pageIndex)) pageIndex))) ;; if the value of the map (vector) has the targetID, true


;; finds all incoming page IDs 
(defn find-inpages [map]
  (combine-maps (for [[pageID outpages] map] ;; for every entry in the map
                  (into {} {pageID (into '[] (remove nil? (search-map pageID map)))})))) ;; into '[] converts seq to vec

;; counts outgoing links for each page ID
(defn count-outpages [map]
   (combine-maps (for [[pageID outpages] map] ;; for every entry in the map
                   (combine-maps (into {} {pageID (count (get map pageID))})))));; counts how many outpages there are


(defn vec-to-map [sequence]
  (combine-maps (for [i sequence]
                  (into {} {(first i) (second i)}))))


(defn split-map [n map] 
    (partition-all (/ (count (keys map)) n) map))


;; calculates PageRank
(defn calc-page-rank [ inpagesMap outpagesCountMap pageRankMap]
  (def d 0.85) ;; damping factor
  (def p (- 1 d)) ;; probability of giving up
  ;(printme myMap)
  
  (combine-maps (for [[pageID inpages] inpagesMap] ;; for every entry in the map
                  (into {} {pageID  (+ p (* d (reduce + (for [i inpages]
                                                          (/ (get pageRankMap i) (get outpagesCountMap i))))))})))) 
  
;; modified pmap from source code
(defn modded-pmap  
  {:added "1.0"
   :static true}
  ([f threadcount coll]
   (let [n threadcount
         rets (map #(future (f %)) coll)
         step (fn step [[x & xs :as vs] fs]
                (lazy-seq
                  (if-let [s (seq fs)]
                    (cons (deref x) (step xs (rest s)))
                    (map deref vs))))]
     (step rets (drop n rets))))
  ([f threadcount coll & colls]
   (let [step (fn step [cs]
                (lazy-seq
                  (let [ss (map seq cs)]
                    (when (every? identity ss)
                      (cons (map first ss) (step (map rest ss)))))))]
     (modded-pmap #(apply f %) (step (cons coll colls))))))

 
(defn main-body [myMap]  
  (def inpagesMap (find-inpages myMap))
  (def outpagesCountMap (count-outpages myMap))
 
  (def threads [1 2 4 8 16 20 32 64])
  (def mapSplits [1 2 4 8 16 20 32 64 128 256 512 1024 2048 4096 8192 10000])
  
  ;(def mapPartitions (map vec-to-map (split-map 10000 inpagesMap))) ;; returns a sequence of maps
  
  (doseq [s mapSplits]
    (doseq [t threads]
      (save-times t s (with-out-str (time 
                                      (do
                                        (def mapPartitions (map vec-to-map (split-map s inpagesMap))) ;; returns a sequence of maps
                                        
                                        (if (= t 1)
                                          ;(def pageRankMap (combine-maps (calc-page-rank inpagesMap outpagesCountMap (start-rank myMap 1)))) ;; initialize page ranks to 1 (no 'map')
                                          (def pageRankMap (combine-maps (map #(calc-page-rank % outpagesCountMap (start-rank myMap 1)) mapPartitions))) ;; initialize page ranks to 1 (standard map)
                                          
                                          ;; else
                                          ;(def pageRankMap (combine-maps (pmap #(calc-page-rank % outpagesCountMap (start-rank myMap 1)) mapPartitions))) ;; initialize page ranks to 1 (standard pmap)
                                          (def pageRankMap (combine-maps (modded-pmap #(calc-page-rank %  outpagesCountMap (start-rank myMap 1)) t mapPartitions)))) ;; initialize page ranks to 1 (modded pmap)
                                        (save-results pageRankMap 1 false) ;; set to true if you want to save the page ranks for 1 pass
                                        
                                        (loop [i 1] ;; already run once above
                                          (if (< i 1000)
                                            (do
                                              (if (= t 1)
                                                ;(def pageRankMap (combine-maps (calc-page-rank inpagesMap outpagesCountMap pageRankMap))) ;; 
                                                (def pageRankMap (combine-maps (map #(calc-page-rank % outpagesCountMap pageRankMap) mapPartitions))) ;; standard map
                                                
                                                ;; else
                                                ;(def pageRankMap (combine-maps (pmap #(calc-page-rank % outpagesCountMap pageRankMap) mapPartitions))) ;; standard pmap
                                                (def pageRankMap (combine-maps (modded-pmap #(calc-page-rank %  outpagesCountMap pageRankMap) t mapPartitions)))) ;; modded pmap
                                              (recur (+ i 1)))
                                            (save-results pageRankMap i false))))))))) ;; set to true if you want to save the page ranks for 1000 passes
  
  (spit "resources/output_times.txt" (str "End: " (java.util.Date.)  "\n\n")   :append true)    
  
  (shutdown-agents)) ;; releases threads


;; converts a string vector to an integer vector
(defn convert-to-int-vec
  [oldVec]
  (loop [newVec oldVec final-vec []]
    (if (empty? newVec)
      final-vec
      (let [[index & remaining] newVec]
        (recur remaining
          (into final-vec [(edn/read-string index)]))
        ))))

;; adds each line (pageID) from the file into a vector of vectors (strings)
(defn file-input-to-map [file] 
  (let [page (line-seq (clojure.java.io/reader file))
        page-number (vec (mapv #(str/split %1 #" ") page))] 
    
    ;; converts the vectors to maps then calls combine-maps
    (main-body (combine-maps (for [i page-number]  
                               {  (edn/read-string (first i))  
                                (convert-to-int-vec (drop 1 i))})))))


(defn run [file]
  (spit "resources/output_times.txt" (str "Start: " (java.util.Date.)  "\n")   :append true)  
  (file-input-to-map file))



(defn -main 
  [& args]
   
  (run "resources/pages.txt"))
 


  

