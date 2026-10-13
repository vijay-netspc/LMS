<div align="center">

# 🎓 LearnHub – Event-Driven Learning Management System

**Spring Boot microservices · RabbitMQ · Saga · CQRS · Event Sourcing · React**

![Java](https://img.shields.io/badge/Java-17-orange?logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-6DB33F?logo=springboot&logoColor=white)
![Spring Cloud](https://img.shields.io/badge/Spring%20Cloud-Gateway%20%7C%20Eureka-6DB33F?logo=spring&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-events-FF6600?logo=rabbitmq&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-8-4479A1?logo=mysql&logoColor=white)
![React](https://img.shields.io/badge/React-18%20%2B%20Vite-61DAFB?logo=react&logoColor=black)
![Tests](https://img.shields.io/badge/unit%20tests-111%20passing-brightgreen)

Mini project for **Scalable Microservices** – topic 15: *Learning Management System*
(User, Course, Enrollment, Assessment, Progress, Certificate; course-completion events).

<img src="docs/screenshots/08-course-completed.png" alt="Course completed – progress 100%, assessment passed, certificate issued" width="900">

</div>

Nine Spring Boot services sit behind an API gateway and talk to each other through RabbitMQ events.
A student enrolls, completes modules and passes an assessment; **progress-service** (CQRS + Event Sourcing)
decides *progress = 100 % AND passed = true* and publishes `CourseCompleted`, which automatically produces a
**PDF certificate with a QR code**, completes the enrollment and notifies the student. Failures are handled by a
**choreographed saga** with compensating transactions. Everything runs **locally without Docker**.

**Patterns:** Database-per-service · API Gateway · Service discovery · Saga · CQRS · Event Sourcing ·
Circuit breaker · Idempotent consumers · Dead-letter queues · JWT stateless auth

---

## 📸 Visual tour

### Student journey

| | |
|:---:|:---:|
| <img src="docs/screenshots/01-home.png" width="440"><br>**Home** – the event flow explained, demo accounts | <img src="docs/screenshots/02-login.png" width="440"><br>**Login** – JWT issued by user-service, checked at the gateway |
| <img src="docs/screenshots/03-course-catalog.png" width="440"><br>**1. Browse & enroll** – `POST /api/enrollments` publishes `StudentEnrolled` | <img src="docs/screenshots/04-my-learning-enrolled.png" width="440"><br>**2. My Learning** – progress row created *asynchronously* by progress-service |
| <img src="docs/screenshots/05-course-progress-and-event-timeline.png" width="440"><br>**3. Learn** – each *Mark module complete* publishes `ModuleCompleted`; the timeline shows the event store | <img src="docs/screenshots/06-assessment.png" width="440"><br>**4. Assessment** – auto-scored, publishes `AssessmentCompleted` |
| <img src="docs/screenshots/07-assessment-result.png" width="440"><br>**5. Result** – 80 % ≥ pass mark 60 % | <img src="docs/screenshots/08-course-completed.png" width="440"><br>**6. Decision** – 100 % **AND** passed → `CourseCompleted` |
| <img src="docs/screenshots/11-certificates.png" width="440"><br>**7. Certificate issued** by certificate-service from the event | <img src="docs/screenshots/12-certificate-pdf.png" width="440"><br>**8. Generated PDF** (OpenPDF) with a QR verification code |
| <img src="docs/screenshots/14-public-certificate-verification.png" width="440"><br>**9. Public verification** – no login, `GET /api/certificates/verify/{code}` | <img src="docs/screenshots/13-notifications.png" width="440"><br>**10. Notifications** – from `StudentEnrolled`, `CourseCompleted`, `CertificateGenerated` |

### Event Sourcing & CQRS

| | |
|:---:|:---:|
| <img src="docs/screenshots/09-event-sourced-timeline.png" width="440"><br>**Event timeline** – the immutable `event_store` history (v1…v7) of one student's course | <img src="docs/screenshots/18-admin-rebuild-read-model.png" width="440"><br>**Rebuild read model** – admin wipes the query table and replays every event |

### Instructor & admin

| | |
|:---:|:---:|
| <img src="docs/screenshots/15-instructor-courses.png" width="440"><br>**Instructor courses** | <img src="docs/screenshots/16-instructor-manage-modules.png" width="440"><br>**Manage modules, lessons & assessment** |
| <img src="docs/screenshots/17-instructor-student-progress.png" width="440"><br>**Student progress** (CQRS read model per course) | <img src="docs/screenshots/10-my-learning-completed.png" width="440"><br>**Completed courses** on the student dashboard |

### Under the hood

| | |
|:---:|:---:|
| <img src="docs/screenshots/19-eureka-dashboard.png" width="440"><br>**Eureka** – all services registered | <img src="docs/screenshots/21-rabbitmq-exchange-bindings.png" width="440"><br>**RabbitMQ** – topic exchange `lms.events` and its routing-key bindings |
| <img src="docs/screenshots/20-rabbitmq-queues.png" width="440"><br>**Queues + dead-letter queues** (`*.dlq`), all drained | <img src="docs/screenshots/23-gateway-circuit-breakers.png" width="440"><br>**Circuit breaker OPEN** after notification-service was stopped → instant 503 fallback |
| <img src="docs/screenshots/22-swagger-progress-service.png" width="440"><br>**Swagger / OpenAPI** for every service | <img src="docs/screenshots/24-e2e-demo-terminal.png" width="440"><br>**`./scripts/e2e-demo.sh`** – the whole workflow incl. saga compensation |

---

## 1. Architecture

```
                     React UI :5173
                          | REST (JWT)
                          v
            API Gateway :8080  (routing via Eureka, JWT check, CORS, circuit breaker per service)
                          |
   +--------+--------+----+-----+----------+----------+-----------+-------------+
   v        v        v          v          v          v           v             v
 user    course  enrollment  content   assessment  progress   certificate   notification
 :8081   :8082     :8083      :8084      :8085      :8086       :8087          :8088
   |        |        |          |          |          |           |             |
 user_db course_db enroll_db content_db assess_db progress_db cert_db    notification_db   (MySQL, one DB each)
                     \          |          |        / | \          |             |
                      \_________|__________|_______/  |  \_________|_____________/
                                RabbitMQ topic exchange "lms.events" (+ DLX / *.dlq)
 Eureka :8761 (service registry)                     event_store (Event Sourcing)
```

| Service | Port | Responsibility | Publishes | Consumes |
|---|---|---|---|---|
| discovery-server | 8761 | Eureka registry | – | – |
| api-gateway | 8080 | Single entry point, JWT, CORS, Resilience4j circuit breakers | – | – |
| user-service | 8081 | Register / login (JWT), users, roles | – | – |
| course-service | 8082 | Course catalogue | – | – |
| enrollment-service | 8083 | Enrollments, **saga participant** | StudentEnrolled | EnrollmentFailed, CourseCompleted |
| content-service | 8084 | Modules, lessons, module completion | ModuleCompleted | – |
| assessment-service | 8085 | Assessments, questions, auto-scoring | AssessmentCompleted | – |
| progress-service | 8086 | **CQRS + Event Sourcing**, completion decision, **saga** | CourseCompleted, EnrollmentFailed | StudentEnrolled, ModuleCompleted, AssessmentCompleted |
| certificate-service | 8087 | PDF certificate + QR code, public verification | CertificateGenerated | CourseCompleted |
| notification-service | 8088 | In-app notifications + e-mail (or log) | – | StudentEnrolled, CourseCompleted, CertificateGenerated |
| frontend | 5173 | React SPA | – | – |

## 2. The event-driven workflow

```
Student ─enroll─> Enrollment ──StudentEnrolled──> Progress (init 0%, asks Content for #modules)
                                              └─> Notification ("enrollment confirmed")
Student ─complete module─> Content ──ModuleCompleted──> Progress (x/y modules → %)
Student ─submit─> Assessment (score) ──AssessmentCompleted──> Progress
Progress: DECISION  progress = 100%  AND  assessment passed = TRUE
          └─yes─> CourseCompleted ──> Certificate (PDF+QR) ──CertificateGenerated──> Notification
                                  ──> Enrollment (status COMPLETED)
                                  ──> Notification ("congratulations")
Saga compensation: Progress cannot initialise (content-service down / course has no modules)
          ──EnrollmentFailed──> Enrollment (ACTIVE → CANCELLED)
```

Event envelope (same JSON in every service): `{eventId, eventType, timestamp, aggregateId: "<studentId>-<courseId>", payload}`.
Full payload contracts: [docs/CONTRACTS.md](docs/CONTRACTS.md).

| Routing key | Queue(s) |
|---|---|
| student.enrolled | progress.queue, notification.queue |
| module.completed | progress.queue |
| assessment.completed | progress.queue |
| course.completed | certificate.queue, notification.queue, enrollment.queue |
| certificate.generated | notification.queue |
| enrollment.failed | enrollment.queue |

## 3. How the patterns are implemented

**Database per service** – 8 MySQL schemas (`lms_*_db`), no cross-database foreign keys; services share data only
through REST (with the caller's JWT, load-balanced via Eureka) or events.

**Saga (choreography)** – `enrollment-service` creates the enrollment `ACTIVE` and publishes `StudentEnrolled`.
`progress-service` tries to initialise progress; if content-service is unreachable or the course has no modules it
publishes `EnrollmentFailed`, and enrollment-service runs the **compensating transaction** (→ `CANCELLED`).
The happy path ends with `CourseCompleted` → enrollment `COMPLETED`.

**Event Sourcing** – `progress-service` never updates progress in place. Every change is appended to the
`event_store` table (`aggregate_id`, `event_type`, `event_data` JSON, `event_version`; unique
`(aggregate_id, event_version)` = optimistic concurrency). `CourseProgressAggregate` is rebuilt by replaying its
events before each command. `GET /api/progress/events/{aggregateId}` shows the history and
`POST /api/progress/rebuild` (admin) wipes the read model and rebuilds it purely from the event store.

**CQRS** – write side `ProgressCommandService` (event listener → aggregate → event store) is separated from the
read side `ProgressQueryService`, which only reads the denormalised `progress` read model maintained by
`ProgressProjector`.

**Reliability** – events are published only after the DB transaction commits; consumers retry 3× with back-off
and then dead-letter to `<queue>.dlq`; every consumer is idempotent (`processed_events` table keyed by `eventId`).

**Resilience / scalability** – the gateway wraps every route in a Resilience4j **circuit breaker**
(opens at 50 % failures, 15 s) with a JSON 503 fallback (`/actuator/circuitbreakers` shows state). Services are
stateless (JWT), discovered by name, and called through client-side load balancing, so you can start a second
instance of any service on another port (`java -jar target/x.jar --server.port=9084`) and Eureka + the gateway
balance between them; RabbitMQ consumers on the same queue become competing consumers automatically.

**Security** – JWT (HS256) issued by user-service, checked at the gateway and again in each service (roles
STUDENT / INSTRUCTOR / ADMIN with method-level `@PreAuthorize`). Background listeners that must call another
service use a short-lived **service token** (progress → content).

## 4. Prerequisites (install once)

| Tool | Version | macOS | Windows |
|---|---|---|---|
| Java JDK | 17+ | `brew install openjdk@17` | `winget install EclipseAdoptium.Temurin.17.JDK` |
| Maven | 3.9+ | `brew install maven` | `winget install Apache.Maven` |
| Node.js | 18+ | `brew install node` | `winget install OpenJS.NodeJS.LTS` |
| MySQL | 8.x | `brew install mysql && brew services start mysql` | MySQL Installer from dev.mysql.com |
| RabbitMQ | 3.12+ | `brew install rabbitmq && brew services start rabbitmq` | Erlang + RabbitMQ installers from rabbitmq.com/docs/install-windows |

After installing:
* MySQL root password must be `root` – or export `DB_PASSWORD=<yours>` before starting.
  (`mysql -uroot -e "ALTER USER 'root'@'localhost' IDENTIFIED BY 'root';"`)
  The databases are created automatically on first start (`sql/init.sql` is optional).
* RabbitMQ management UI: `rabbitmq-plugins enable rabbitmq_management` → http://localhost:15672 (guest/guest).

## 5. Run

### One command (macOS / Linux / Git Bash)
```bash
./scripts/start-all.sh      # builds all services, starts Eureka -> services -> gateway -> React
./scripts/e2e-demo.sh       # runs the whole workflow through the gateway and prints each step
./scripts/stop-all.sh
```
Logs are in `logs/<service>.log`. Options: `SKIP_BUILD=1`, `NO_FRONTEND=1`.

### Windows PowerShell
```powershell
.\scripts\start-all.ps1          # -SkipBuild, -NoFrontend
.\scripts\stop-all.ps1
```

### Manually (one terminal each, in this order)
```bash
cd discovery-server && mvn spring-boot:run          # wait until http://localhost:8761 is up
cd user-service && mvn spring-boot:run              # same for course-, enrollment-, content-, assessment-,
                                                     # progress-, certificate-, notification-service
cd api-gateway && mvn spring-boot:run
cd frontend && npm install && npm run dev
```
Run unit tests: `mvn test` inside any service folder (no database or broker needed).

## 6. URLs

| What | URL |
|---|---|
| React app | http://localhost:5173 |
| API Gateway | http://localhost:8080 |
| Eureka dashboard | http://localhost:8761 |
| RabbitMQ UI | http://localhost:15672 (guest / guest) – watch queues, DLQs |
| Swagger UI | http://localhost:808X/swagger-ui.html (X = 1…8; click **Authorize**, paste JWT) |
| Circuit breakers | http://localhost:8080/actuator/circuitbreakers |

## 7. Demo accounts (seeded on first start)

| Email | Password | Role |
|---|---|---|
| admin@example.com | Admin@123 | ADMIN |
| instructor@example.com | Instructor@123 | INSTRUCTOR (id 2) |
| instructor2@example.com | Instructor@123 | INSTRUCTOR (id 3) |
| student1@example.com / student2 / student3 | Student@123 | STUDENT |

Seed data: 5 published courses, 3–4 modules per course with 2–3 lessons each, one assessment per course
(5 questions × 20 marks, pass mark 60 %).

## 8. Demo script (what to show in a viva)

1. Eureka dashboard: all 9 applications registered.
2. React: log in as `student1`, enroll in a course → *My Learning* shows "Not started" (progress was created
   asynchronously by an event). RabbitMQ UI shows the message flow.
3. Open the course, *Mark module complete* for each module → progress bar moves; the **Event timeline** panel shows
   the event-sourced history.
4. Take the assessment: fail once (course stays *Awaiting assessment*), then pass → status **Completed**,
   enrollment **COMPLETED**, certificate appears under *Certificates* (download PDF, scan QR / open verify page),
   notifications bell shows the e-mails.
5. Saga: as instructor create a published course with no modules; as student enroll → enrollment becomes
   **CANCELLED** (compensation, see `enrollment-service` log "SAGA COMPENSATION").
6. Event sourcing: as admin click *Rebuild progress read model* → read model rebuilt from `event_store`.
7. Circuit breaker: stop notification-service, call `/api/notifications` a few times → instant 503 fallback,
   `/actuator/circuitbreakers` shows `OPEN`.

`./scripts/e2e-demo.sh` automates steps 2–5 and prints every stage.

## 9. API summary (all through the gateway)

| Method | Path | Access |
|---|---|---|
| POST | /api/users/register, /api/users/login | public |
| GET/PUT/DELETE | /api/users/{id}, GET /api/users | self / ADMIN |
| GET/POST/PUT/DELETE | /api/courses, /api/courses/{id}, /api/courses/instructor/{id} | students see PUBLISHED; owner instructor / ADMIN write |
| POST | /api/enrollments | STUDENT (self) / ADMIN |
| GET | /api/enrollments/student/{id}, /course/{id}, /{id}; PUT /{id}/cancel | owner / INSTRUCTOR / ADMIN |
| CRUD | /api/modules, /api/lessons | owner INSTRUCTOR / ADMIN write; authenticated read |
| POST | /api/modules/{id}/complete | enrolled STUDENT → ModuleCompleted |
| GET | /api/modules/course/{courseId}/completed | STUDENT (own completed module ids) |
| CRUD | /api/assessments, /api/assessments/{id}/questions | owner INSTRUCTOR / ADMIN (students never see answers) |
| POST | /api/assessments/{id}/submit | enrolled STUDENT → AssessmentCompleted |
| GET | /api/progress/student/{id}, /student/{id}/course/{courseId} | self / INSTRUCTOR / ADMIN |
| GET | /api/progress/course/{courseId} | INSTRUCTOR / ADMIN |
| GET | /api/progress/events/{studentId-courseId} | self / INSTRUCTOR / ADMIN (event store) |
| POST | /api/progress/rebuild | ADMIN (replay event store) |
| GET | /api/certificates/student/{id}, /{id}, /{id}/download (PDF) | owner / INSTRUCTOR / ADMIN |
| GET | /api/certificates/verify/{code} | **public** |
| GET | /api/notifications/user/{id}, /user/{id}/unread-count; PUT /{id}/read | self / ADMIN |
| GET | /api/notifications | ADMIN |

Errors always have the shape `{timestamp, status, message, path}`.

## 10. Configuration

All settings have defaults; override with environment variables (see `.env.example`): `DB_HOST/PORT/USERNAME/PASSWORD`,
`RABBITMQ_HOST/PORT/USER/PASS`, `EUREKA_URL`, `JWT_SECRET` (identical for all, ≥ 32 chars), `CORS_ALLOWED_ORIGINS`,
`CERTIFICATE_VERIFY_BASE_URL`, and for real e-mails `MAIL_ENABLED=true MAIL_HOST MAIL_PORT MAIL_USERNAME MAIL_PASSWORD`
(otherwise e-mails are logged and stored with status `LOGGED`).

## 11. Troubleshooting

| Problem | Fix |
|---|---|
| `start-all.sh`: MySQL / RabbitMQ not reachable | Start them (`brew services start mysql rabbitmq`, or Windows Services) |
| `Access denied for user 'root'` | Set `DB_PASSWORD` to your MySQL root password |
| Port already in use | Another process uses 8080–8088/8761/5173 (`lsof -i :8080`, Windows `netstat -ano \| findstr :8080`) |
| 503 "… is currently unavailable" from gateway | Service down or not yet registered in Eureka (wait ~30 s after start) |
| Progress stays "Initialising…" | progress-service not running, or check `progress.queue.dlq` in RabbitMQ UI and `logs/progress-service.log` |
| Enrollment becomes CANCELLED immediately | Expected saga compensation: the course has no modules (or content-service was down) |
| 401 everywhere | Token expired (log in again) or `JWT_SECRET` differs between services |
| CORS error in browser | Frontend must run on http://localhost:5173 (or set `CORS_ALLOWED_ORIGINS`) |

## 12. Project structure

```
lms-project final/
├── discovery-server/  api-gateway/
├── user-service/  course-service/  enrollment-service/  content-service/  assessment-service/
├── progress-service/       aggregate/ (event-sourced aggregate), projection/, service/ (command vs query)
├── certificate-service/    pdf/ (OpenPDF + ZXing QR)
├── notification-service/   mail/
├── frontend/               React + Vite
├── docs/CONTRACTS.md       event & port contracts
├── docs/screenshots/       images used in this README
├── scripts/                start-all / stop-all (.sh, .ps1), e2e-demo.sh
└── sql/init.sql            optional DB creation
```

## 13. Known limitations / future work

* No transactional outbox: if RabbitMQ is down exactly at commit time the event is logged as failed, not resent.
* Shared HMAC JWT secret (an OAuth2 authorization server with RS256 would be the production choice).
* Single MySQL server hosting all schemas (logically separated); in production each service would get its own instance.
* Centralised config (Spring Cloud Config) and distributed tracing (Micrometer + Zipkin) are not included.
