# اقرأ هذا أولًا — مرآة R4_F33 العامة لمراجعة رحلة الإشارة

هذه ليست نسخة تشغيلية من «عين المضارب»، بل **مرآة عامة منقّحة لمراجعة أحدث رحلة إشارة R4_F33**.

هوية النسخة الأصلية التي اشتُقت منها المراجعة:

`R4_F33 Canonical Exact SHA-256 = 30ac5a9844c5b929ba4a5616a9d0f821bba704fad445229c50a63a3cb3b025ae`

الهدف أن يستطيع الخبير تتبع أكبر قدر ممكن من الـcontrol flow الحقيقي:

`المصدر الفيزيائي → observation → scheduler → durable admission/lease → Probability request/worker/result → Episode/Seal → Terminal truth → Decision/UI`

مع عدم نشر النموذج المدرب أو الأسرار أو بيانات الجلسات الحية.

## ابدأ بهذا الترتيب

1. `R4_F33_MIRROR_STATUS.md` — يحدد بدقة ما هو مطابق byte-for-byte لـR4 وما هو historical فقط.
2. `SIGNAL_JOURNEY_MAP.md` — خارطة الرحلة من أول المصدر حتى UI.
3. `REVIEW_QUESTIONS.md` — أسئلة التدقيق العدائي المقترحة.
4. `01_source/`
5. `02_observation/`
6. `03_signal_probability/`
7. `04_lifecycle/`
8. `05_terminal_projection/`
9. `06_contracts/`
10. `08_r4_f33_authority/` — السلطات الجديدة المهمة في R4/F32/F33.
11. `07_r4_f33_review_tests/` — اختبارات F32/F33 المنشورة للمراجعة.

## النموذج

`03_signal_probability/gann20_probability_model.py`

هو Stub عام **غير قادر على scoring**. تم حذف:
- model weights/trees/artifacts؛
- وصفة الميزات الحساسة؛
- معاملات المعايرة الإنتاجية.

لكن مسار طلب الاحتمال، الـIPC، worker/result authority، lifecycle والـterminal surrounding code متاح للمراجعة بالقدر الموضح في Status.

## الأسرار والبيانات

غير منشور عمدًا:
- Secret Vault/API keys/credentials؛
- حسابات أو Broker connectors الحساسة؛
- live session evidence؛
- historical market datasets؛
- مسارات جهاز المستخدم؛
- حزم Acceptance/Production الكاملة.

## سؤال المراجعة الأساسي

لا تبحث فقط عن Exception. اختر حقيقة/إشارة واحدة وحاول كسر سلسلة السببية:

- هل يمكن فقد observation أو تكرارها؟
- هل يمكن ربط Probability بشمعة/episode/physical generation خاطئة؟
- هل يمكن scheduler/admission أن يترك دينًا بلا accounting؟
- هل يمكن late result أن يكتب فوق حقيقة أحدث؟
- هل يمكن restart أن يبعث Episode بعد Terminal؟
- هل يمكن UI/Decision أن يسبق durable truth؟

أي Finding يعتمد على ملف معلّم `LEGACY/HISTORICAL` يجب التحقق منه لاحقًا ضد الـCanonical R4_F33 قبل اعتماده كعيب إنتاجي.
