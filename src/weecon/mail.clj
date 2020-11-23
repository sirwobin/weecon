(ns weecon.mail
  (:require [clojure.string :as string]
            [schema.core :as schema]
            [weecon.output])
  (:import (javax.mail Message Message$RecipientType Session Authenticator PasswordAuthentication Transport)
           (javax.mail.internet InternetAddress MimeMultipart MimeBodyPart)))

(def mail-config-spec-schema {:weecon.output/type                  (schema/eq "smtp-email")
                              :smtp-server                         schema/Str
                              :smtp-port                           schema/Str
                              :to-address                          schema/Str
                              :from-address                        schema/Str
                              :subject                             schema/Str
                              (schema/optional-key :auth-username) schema/Str
                              (schema/optional-key :auth-password) schema/Str
                              (schema/optional-key :auth-method)   (schema/eq "tls")})

(defn- get-session [{smtp-server      :smtp-server
                     smtp-port        :smtp-port
                     auth-username    :auth-username
                     auth-password    :auth-password
                     auth-method      :auth-method
                     :or {smtp-port   25}
                     :as mail-spec}]
  (let [props         (doto (java.util.Properties.)
                        (.put "mail.smtp.host" smtp-server)
                        (.put "mail.smtp.port" smtp-port))
        authenticator (when auth-method
                        (proxy [Authenticator] []
                          (getPasswordAuthentication [] (PasswordAuthentication. auth-username auth-password))))]
    (when auth-method
      (.put props "mail.smtp.auth" "true")
      (when (-> auth-method string/lower-case (= "tls"))
        (.put props "mail.smtp.starttls.enable" "true")))
    (Session/getInstance props authenticator)))

(defn smtp-send [{to-address        :to-address
                  from-address      :from-address
                  subject           :subject
                  body-content      :body-content
                  body-content-type :body-content-type
                  attach-file-path  :attach-file-path
                  :as mail-spec}]
  (let [session         (get-session mail-spec)
        msg             (doto (javax.mail.internet.MimeMessage. session)
                          (.setFrom (InternetAddress. from-address))
                          (.setSubject subject)
                          (.addRecipient Message$RecipientType/TO (InternetAddress. to-address))
                          (.setSentDate (java.util.Date.)))
        body-part       (doto (MimeBodyPart.)
                          (.setText body-content))
        content         (doto (MimeMultipart.)
                          (.addBodyPart body-part))]
    (when body-content-type
      (.addHeader body-part "Content-Type" body-content-type))
    (when attach-file-path
      (.addBodyPart content (doto (MimeBodyPart.)
                              (.attachFile attach-file-path))))
    (.setContent msg content)
    (Transport/send msg)))

(defmethod weecon.output/send! "smtp-email" [output-spec]
  (smtp-send output-spec))

(comment
  (let [address   (System/getenv "MAIL_ADDRESS")
        mail-spec {:to-address        address
                   :from-address      address
                   :subject           "javax.mail test"
                   :body-content      "blah <b>blah</b> blah"
                   :body-content-type "text/html"
                   :attach-file-path  "../../Downloads/javamail-samples/sendfile.java"
                   :smtp-server       "smtp.gmail.com"
                   :smtp-port         "587"
                   :auth-method       "tls"
                   :auth-username     address
                   :auth-password     (System/getenv "MAIL_PASSWORD")}]
    (smtp-send mail-spec)))
